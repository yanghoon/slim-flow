package slim.flow.flink.source.websocket;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.connector.source.SourceSplit;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Builder
public class WebSocketSplit implements SourceSplit {

    private static final AtomicInteger seqeunce = new AtomicInteger(0);

    private final String splitId = Integer.toString(seqeunce.getAndIncrement());

    @Getter
    private final String url;
    @Getter
    private final Map<String, String> query;

    @Override
    public String splitId() {
        return splitId;
    }

}
