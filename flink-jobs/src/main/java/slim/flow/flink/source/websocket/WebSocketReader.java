package slim.flow.flink.source.websocket;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import slim.flow.flink.source.common.SimpleSourceReader;

@Slf4j
@RequiredArgsConstructor
public class WebSocketReader implements SimpleSourceReader<String, WebSocketSplit> {

    private final SourceReaderContext readerContext;

    private List<WebSocketSplit> splits = new ArrayList<>();
    private CompletableFuture<Void> available = new CompletableFuture<>();

    // private WebSocketClient client = new WebSocketClient();

    @Override
    public void addSplits(List<WebSocketSplit> splits) {
        log.info("Add splits: {}", splits);
        this.splits.addAll(splits);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        log.info("Poll [splits={}]", splits);
        
        // if (splits.isEmpty()) {
        //     return InputStatus.NOTHING_AVAILABLE;
        // }

        // var split = splits.get(0);
        // client.connect(split.getUrl(), split.getQuery(), message -> {
        //     output.collect(message);
        //     readerContext.signalAvailable();
        // });

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        log.debug("available: {}", available.toString());
        return available;
    }
    
    public static interface Client {
        
    }
}
