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

      def collect
        data_points = []
        tables.each do |table_name|
          metric_list.each do |(metric_name, statistic, unit, reporting_prefix, period, scale)|
            data_point = get_data_point(
              :namespace     => "AWS/DynamoDB",
              :metric_name   => metric_name,
              :statistic     => statistic,
              :unit          => unit,
              :default_value => 0,
              :period        => period,
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
              data_points << data_point
            end
          end
        end
        data_points
      end
    end
  end
end
