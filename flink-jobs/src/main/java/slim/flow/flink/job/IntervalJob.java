package slim.flow.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import slim.flow.flink.source.IntervalSource;

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
        
        stream.print();
        
        var result = env.execute();
        System.out.println(result);
    }

    private static Source<Long, ?, ?> internal(long i) {
        return new IntervalSource();
    }
    
}
