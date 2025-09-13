package slim.flow.flink.v1.source.web;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import slim.flow.flink.v1.source.common.SimpleSourceReader;

public class WebSourceReader<T> implements SimpleSourceReader<T, WebSplit> {

    protected SourceReaderContext readerContext;
    protected List<WebSplit> splits = new ArrayList<>();
    protected CompletableFuture<Void> availability = new CompletableFuture<>();

    public WebSourceReader(SourceReaderContext readerContext) {
        this.readerContext = readerContext;
    }

    @Override
    public void addSplits(List<WebSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (splits.isEmpty()) {
            return InputStatus.MORE_AVAILABLE;
        }

        call(splits.get(0));

        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    protected void call(WebSplit webSplit) throws Exception {
        var uri = new URI(webSplit.getUrl());
        var headers = webSplit.headerMap().entrySet().stream()
                .flatMap(e -> List.of(e.getKey(), e.getValue()).stream())
                .toArray(String[]::new);
        
        var req = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .headers(headers)
                .build();
        
        HttpClient.newHttpClient()
                .sendAsync(req, BodyHandlers.ofString())
                .thenApply(res -> webSplit.parser().apply(res.body()))
                .join();
        
        webSplit.next().apply(webSplit);
    }

}
