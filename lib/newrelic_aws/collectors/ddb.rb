module NewRelicAWS
  module Collectors
    class DDB < Base
      def initialize(access_key, secret_key, region, options)
        super(access_key, secret_key, region, options)
        @tables = options[:tables]
      end

      def tables
        return @tables if @tables
        ddb = AWS::DynamoDB.new(
          :access_key_id => @aws_access_key,
          :secret_access_key => @aws_secret_key,
          :region => @aws_region
        )
        ddb.tables.map { |table| table.name }
      end

      def metric_list
        [
          {:metric_name => "ThrottledRequests", :statistic => "Sum", :unit => "Count", :default_value => 0, :dimensions => [["Operation", "PutItem"], ["Operation", "DeleteItem"], ["Operation", "UpdateItem"], ["Operation", "GetItem"], ["Operation", "BatchGetItem"], ["Operation", "BatchWriteItem"], ["Operation", "Scan"], ["Operation", "Query"]]},
          {:metric_name => "ProvisionedReadCapacityUnits", :statistic => "Sum", :unit => "Count", :period => 300, :reporting_prefix => "Capacity/Read"},
          {:metric_name => "ProvisionedWriteCapacityUnits", :statistic => "Sum", :unit => "Count", :period => 300, :reporting_prefix => "Capacity/Write"},
          {:metric_name => "ConsumedReadCapacityUnits", :statistic => "Sum", :unit => "Count", :scale => 60, :reporting_prefix => "Capacity/Read"},
          {:metric_name => "ConsumedWriteCapacityUnits", :statistic => "Sum", :unit => "Count", :scale => 60, :reporting_prefix => "Capacity/Write"},
        ]
      end

      def derived_metrics
        [
          ["Utilization/ReadUtilization", :ratio, "ConsumedReadCapacityUnits", "ProvisionedReadCapacityUnits"],
          ["Utilization/WriteUtilization", :ratio, "ConsumedWriteCapacityUnits", "ProvisionedWriteCapacityUnits"],
        ]
      end

      def collect
        data_points = []
        tables.each do |table_name|
          table_data_points = []
          metric_list.each do |metric|
            metric_dimensions = []
            if metric[:dimensions]
              for dimension in metric[:dimensions]
                metric_dimensions << [{:name => "TableName", :value => table_name}, {:name => dimension[0], :value => dimension[1]}]
              end
            else
              metric_dimensions = [[{:name => "TableName", :value => table_name}]]
            end
            for dimension in metric_dimensions
              data_point = get_data_point(
                :namespace      => "AWS/DynamoDB",
                :component_name => table_name,
                :metric_name    => metric[:metric_name],
                :statistic      => metric[:statistic],
                :unit           => metric[:unit],
                :default_value  => metric[:default_value],
                :period         => metric[:period],
                :start_time     => (Time.now.utc - (@cloudwatch_delay + (metric[:period] || 60) * 5)).iso8601,
                :dimensions     => dimension,
              )
              unless data_point.nil?
                data_point[1] = metric[:reporting_prefix] + "/" + data_point[1] if metric[:reporting_prefix]
                data_point[1] += "/" + dimension.last[:value] if dimension.size > 1
                data_point[3] /= metric[:scale] if metric[:scale]
              end
              NewRelic::PlatformLogger.debug("metric_name: #{metric[:metric_name]}, statistic: #{metric[:statistic]}, unit: #{metric[:unit]}, dimension: #{dimension.inspect}, response: #{data_point.inspect}")
              unless data_point.nil?
                table_data_points << data_point
              end
            end
          end

          # calculate derived metrics
          derived_metrics.each { |attrs|
            if attrs[1] == :ratio
              # find the required attributes
              data_point1 = table_data_points.select { |p| p[1].match(attrs[2]) }.first
              data_point2 = table_data_points.select { |p| p[1].match(attrs[3]) }.first
              next unless data_point1 && data_point2
              table_data_points << [table_name, attrs[0], "Percent", data_point1[3] * 100.0 / data_point2[3]]
            end
          }
          data_points += table_data_points
        end
        data_points
      end
    end
  end
end
