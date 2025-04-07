package slim.flow.flink.job;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @see https://flink.apache.org/2020/07/30/advanced-flink-application-patterns-vol.3-custom-window-processing/
 * @see https://stackoverflow.com/questions/71777546/how-to-delay-event-processing-with-apache-flink
 * @see https://www.alibabacloud.com/blog/598973
 */
public class S3SinkJob {

    public static void main(String[] args) throws Exception {
        // System.setProperty(ConfigConstants.ENV_FLINK_CONF_DIR, "flink-jobs/bin/conf/");
        // var conf = GlobalConfiguration.loadConfiguration("flink-jobs/bin/main/conf/");
        // var env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var OUT_DIR = "s3a://warehouse/csv/users_csv";
        var source = env.fromData(
            Tuple2.of(1L, "admin"), Tuple2.of(2L, "user")
        );
        var stream = source.map(Tuple2::toString);
        var sink = FileSink.forRowFormat(
                new Path(OUT_DIR),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .build();
        
        // var client = DummyRestClient.create();
        stream.print();
        stream.sinkTo(sink);
        
        var result = env.execute();
        System.out.println(result);
    }

}
