package slim.flow.flink.job;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import slim.flow.flink.client.DummyRestClient;
import slim.flow.flink.source.IntervalSource;

/**
 * @see https://flink.apache.org/2020/07/30/advanced-flink-application-patterns-vol.3-custom-window-processing/
 * @see https://stackoverflow.com/questions/71777546/how-to-delay-event-processing-with-apache-flink
 * @see https://www.alibabacloud.com/blog/598973
 */
public class IntervalJob {

    private static StreamExecutionEnvironment env;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();    
        env.setParallelism(1);

        // DataStreamSource<Object> source = fromSource(null);
        // DataStreamV2SourceUtils.

        // env.socketTextStream(null, 0)
        // env.fromData(null)

        var source = internal(5);
        var stream = env.fromSource(source,
            WatermarkStrategy.noWatermarks(),
            "Interval");
        
        // var client = DummyRestClient.create();
        stream
            .map(
                skip -> DummyRestClient.create().users(skip.intValue(), 1).users,
                TypeInformation.of(new TypeHint<List<Map<String,Object>>>(){})
            )
            .print();
        
        var result = env.execute();
        System.out.println(result);
    }

    private static Source<Long, ?, ?> internal(long i) {
        return new IntervalSource();
    }
    
}
