package slim.flow.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import slim.flow.flink.source.websocket.WebSocketSource;
import slim.flow.flink.source.websocket.WebSocketSplit;

public class WebSocketJob {

    private static final String savepointDir = "file:///tmp/savepoint";

    /*
     * https://websocket.org/tools/websocket-echo-server
     */
    public static void main(String[] args) {
        var config = configWithSavepoint();

        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.disableOperatorChaining();
        env.enableChaepointint(Duration.ofSeconds(30).toMillis());
        env.setDefaultSavepointDirectory(savepointDir);

        var splits = List.of(
            WebSocketSplit.builder().url("wss://echo.websocket.org/").build()
        );
        var source = new WebSocketSource(splits);

        var stream = env
            .setParallelism(splits.size())
            .fromSource(source, WatermarkStrategy.noWatermarks(), "websocket-source")
            .uid("source-websocket");
        
        stream
            .flatMap(WebSocketJob::parse);
            .print().uid("sink-stdout");

        env.execute("Websocket Stream Job");
    }

    private static Configuration configWithSavepoint() {
        var config = new Configuration();

        // var savepointDir = "file:///tmp/savepoint";
        var savepointPath = savepointDir + "/savepoint-xxxxxx-xxxxxxxxxxxx/";
        var savepointSetting = SavepointRestoreSettings.forPath(savepointPath);
        SavepointRestoreSettings.toConfiguration(savepointSetting, config);

        return config;
    }

    private static void parse(String body, Collector<Map<String, String>> out) throws Exception {
        //TODO
    }
    
}
