require 'pry'
module NewRelicAWS
  module Collectors
    class DDB < Base
      def initialize(access_key, secret_key, region, options)
        super(access_key, secret_key, region, options)
        @tables = options[:tables]
        @gsis = {}
      end

      def tables
        return @tables if @tables
        ddb = Aws::DynamoDB::Resource.new(
          :access_key_id => @aws_access_key,
          :secret_access_key => @aws_secret_key,
          :region => @aws_region
        )
        ddb.tables.map { |table| 
          @gsis[table.name] = table.global_secondary_indexes.map { |gsi| gsi.index_name } if table.global_secondary_indexes
          table.name 
        }
      end

      def metric_list
        [
          {:metric_name => "ThrottledRequests", :statistic => "Sum", :unit => "Count", :default_value => 0, :dimensions => [["Operation", "PutItem"], ["Operation", "DeleteItem"], ["Operation", "UpdateItem"], ["Operation", "GetItem"], ["Operation", "BatchGetItem"], ["Operation", "BatchWriteItem"], ["Operation", "Scan"], ["Operation", "Query"]], :ignore_gsi => true},
          {:metric_name => "ProvisionedReadCapacityUnits", :statistic => "Maximum", :unit => "Count", :period => 300, :reporting_prefix => "Capacity/Read"},
          {:metric_name => "ProvisionedWriteCapacityUnits", :statistic => "Maximum", :unit => "Count", :period => 300, :reporting_prefix => "Capacity/Write"},
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

            # If we have GSIs, we want to get data points for each of the same
            # metrics of the table itself, except for any metrics with the
            # :ignore_gsi flag.
            gsi_dimensions = []
            for dimension in metric_dimensions
              break if metric[:ignore_gsi]

              # add a dimension for each existing dimension for each GSI
              break unless @gsis[table_name]
              for gsi in @gsis[table_name]
                gsi_dimension = dimension.dup
                gsi_dimension << {:name => "GlobalSecondaryIndexName", :value => gsi}
                gsi_dimensions << gsi_dimension
              end
            end
            metric_dimensions += gsi_dimensions

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
                if dimension.size > 1
                  if (gsi_dimension = dimension.select { |h| h[:name] == "GlobalSecondaryIndexName" }).size > 0
                    data_point[1] = "GSI/" + data_point[1] + "/" + gsi_dimension.first[:value]
                  else
                    data_point[1] += "/" + dimension.last[:value]
                  end
                end
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
              # find all relevant data points
              numerator_data_points = table_data_points.select { |p| p[1].match(attrs[2]) }
              denominator_data_points = table_data_points.select { |p| p[1].match(attrs[3]) }

              # for each denominator, find the matching numerator metric
              for denominator in denominator_data_points
                if denominator[1].match("GSI/")
                  gsi_name = denominator[1].split('/').last
                  numerator = numerator_data_points.select { |p| p[1].match(gsi_name) }.first
                  metric_name = "GSI/#{attrs[0]}/#{gsi_name}"
                else
                  numerator = numerator_data_points.select { |p| !p[1].match("GSI/") }.first
                  metric_name = attrs[0]
                end

                next unless numerator
                table_data_points << [table_name, metric_name, "Percent", numerator[3] * 100.0 / denominator[3]]
              end
            end
          }
          data_points += table_data_points
        end
        data_points
      end
    end
  end
end
