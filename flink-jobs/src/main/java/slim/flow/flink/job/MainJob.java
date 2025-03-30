package slim.flow.flink.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainJob {

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var source = env.fromSequence(0, 10);
        source.setParallelism(1)
            .map(n -> n * 2)
            .print();

        var result = env.execute();

        System.out.println(result);
    }

}
