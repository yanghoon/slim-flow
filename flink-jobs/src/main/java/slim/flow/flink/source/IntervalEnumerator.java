package slim.flow.flink.source;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntervalEnumerator implements SplitEnumerator<Stateless, Stateless> {

    private static Logger log = LoggerFactory.getLogger(IntervalEnumerator.class);

    private SplitEnumeratorContext<Stateless> enumContext;
    private long initialDelayMillis = 0L;
    private long periodMillis = 1000L;

    public IntervalEnumerator(SplitEnumeratorContext<Stateless> enumContext) {
        this(enumContext, null);
    }

    public IntervalEnumerator(SplitEnumeratorContext<Stateless> enumContext, Stateless checkpoint) {
        this.enumContext = enumContext;
    }

    @Override
    public void start() {
        log.info("start(,,initialDelayMillis={},periodMillis={})", initialDelayMillis, periodMillis);

        enumContext.callAsync(
            () -> {
                log.info("start() -> callAsync() -> callable()");
                return System.currentTimeMillis();
            },
            (v, e) -> {
                log.info("start() -> callAsync() -> handler({}, {})", v, e);
            },
            initialDelayMillis,
            periodMillis);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleSplitRequest'");
    }

    @Override
    public void addSplitsBack(List<Stateless> splits, int subtaskId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addSplitsBack'");
    }

    @Override
    public void addReader(int subtaskId) {
        log.info("subtaskId : {}", subtaskId);
    }

    @Override
    public Stateless snapshotState(long checkpointId) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'snapshotState'");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }
    
}
