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
          ["ReadThrottleEvents", "Sum", "Count", "Throttled", 0],
          ["WriteThrottledEvents", "Sum", "Count", "Throttled", 0],
          ["ProvisionedReadCapacityUnits", "Sum", "Count", "Capacity/Read", nil, 300],
          ["ProvisionedWriteCapacityUnits", "Sum", "Count", "Capacity/Write", nil, 300],
          ["ConsumedReadCapacityUnits", "Sum", "Count", "Capacity/Read", 0, nil, 60],
          ["ConsumedWriteCapacityUnits", "Sum", "Count", "Capacity/Write", 0, nil, 60],
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
          metric_list.each do |(metric_name, statistic, unit, reporting_prefix, default_value, period, scale)|
            data_point = get_data_point(
              :namespace     => "AWS/DynamoDB",
              :metric_name   => metric_name,
              :statistic     => statistic,
              :unit          => unit,
              :default_value => default_value,
              :period        => period,
              :start_time    => (Time.now.utc - (@cloudwatch_delay + (period || 60) * 5)).iso8601,
              :dimension     => {
                :name  => "TableName",
                :value => table_name
              }
            )
            unless data_point.nil?
              data_point[1] = reporting_prefix + "/" + data_point[1] if reporting_prefix
              data_point[3] /= scale if scale
            end
            NewRelic::PlatformLogger.debug("metric_name: #{metric_name}, statistic: #{statistic}, unit: #{unit}, response: #{data_point.inspect}")
            unless data_point.nil?
              table_data_points << data_point
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
