package slim.flow.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import slim.flow.flink.source.websocket.WebSocketSource;
import slim.flow.flink.source.websocket.WebSocketSplit;

public class WebSocketJob {

    /*
     * https://websocket.org/tools/websocket-echo-server
     */
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var source = new WebSocketSource(
            WebSocketSplit.builder().url("wss://echo.websocket.org/").build()
        );

        var stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "websocket-source");
        stream.print();
    }
    
}
