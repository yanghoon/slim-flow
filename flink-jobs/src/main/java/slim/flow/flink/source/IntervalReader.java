package slim.flow.flink.source;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntervalReader implements SourceReader<Long, Stateless> {

    private static Logger log = LoggerFactory.getLogger(IntervalReader.class);

    private SourceReaderContext readerContext;
    private long periodMillis;
    private Executor executor;
    private AtomicLong counter;

    public IntervalReader(SourceReaderContext readerContext) {
        this.readerContext = readerContext;
        this.periodMillis = 1000L;
        this.executor = CompletableFuture.delayedExecutor(periodMillis, TimeUnit.MILLISECONDS);
        this.counter = new AtomicLong();
    }

    @Override
    public void start() {
        log.info("");
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Long> output) throws Exception {
        var val = counter.incrementAndGet();
        output.collect(val);
        log.info("pollNext() -> {}", val);
        return InputStatus.NOTHING_AVAILABLE; // isAvailable() -> pollNext()
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        log.info("isAvailable()");
        return CompletableFuture.runAsync(() -> log.info("isAvailable() -> done"), this.executor);
    }

    @Override
    public List<Stateless> snapshotState(long checkpointId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'snapshotState'");
    }

    @Override
    public void addSplits(List<Stateless> splits) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addSplits'");
    }

    @Override
    public void notifyNoMoreSplits() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'notifyNoMoreSplits'");
    }

    @Override
    public void close() throws Exception {
        log.info("");
    }

}
