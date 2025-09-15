package slim.flow.flink.v1.source.web;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import slim.flow.flink.v1.source.common.SimpleSourceReader;

@Slf4j
public class WebSourceReader<T> implements SimpleSourceReader<T, WebSplit> {

    // for SourceReader
    protected SourceReaderContext readerContext;
    protected List<WebSplit> splits = new ArrayList<>();
    protected CompletableFuture<Void> availability = new CompletableFuture<>();

    // for WebRequest
    private Executor delayer = CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS);
    private AtomicLong requestCount = new AtomicLong(0);

    public WebSourceReader(SourceReaderContext readerContext) {
        this.readerContext = readerContext;
    }

    @Override
    public void start() {
        readerContext.sendSplitRequest();
    }

    @Override
    public void addSplits(List<WebSplit> splits) {
        log.debug("Add Splits: {}", splits);
        log.info("Current Splits: {}", this.splits);
        this.splits.addAll(splits);

        availability.complete(null);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (splits.isEmpty()) {
            log.info("No splits available.");
            return InputStatus.NOTHING_AVAILABLE;
        }

        call(splits.get(0), output);

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public List<WebSplit> snapshotState(long checkpointId) {
        log.info("Checkpoint: {}, Splits: {}", checkpointId, splits);
        return splits;
    }

    protected void call(WebSplit webSplit, ReaderOutput<T> output) throws Exception {
        log.info("Calling HTTP Request: {}", webSplit);
        var uri = webSplit.getUrl();
        var headers = webSplit.headers();

        var req = HttpRequest.newBuilder()
                .GET()
                .uri(new URL(uri))
                .headers(headers)
                .build();
        
        var requestId = requestCount.incrementAndGet();
        log.info("Request #{}: {}", requestId, req);
        log.info("Request #{}-Headers: {}", requestId, headers);

        HttpClient.newHttpClient()
                .sendAsync(req, BodyHandlers.ofString())
                .thenApply(res -> webSplit.parser().apply(res.body()))
                .thenAccept(itmes -> {
                    if (itmes.isEmpty()) {
                        availability = CompletableFuture.runAsync(() -> {}, delayer);
                        return;
                    }

                    items.forEach(outptu::collect);
                    webSplit.setItems(items);
                    split.next().apply(webSplit);
                    availability.complete(null);
                })
                .join();
    }

}
