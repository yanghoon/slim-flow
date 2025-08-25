package slim.flow.flink.source.rest;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Main RestSource class that implements the FLIP-27 Source interface.
 * This source reads data from REST APIs with pagination support.
 */
public class RestSource implements Source<String, RestSourceSplit, RestSourceEnumeratorState> {
    
    private final String baseUrl;
    private final int pageSize;
    private final long pollingIntervalMs;
    private final boolean isBounded;
    private final Duration requestTimeout;
    private final Map<String, String> headers;
    
    /**
     * Creates a bounded RestSource.
     */
    public static RestSource bounded(String baseUrl, int pageSize) {
        return new RestSource(baseUrl, pageSize, 0L, true, Duration.ofSeconds(30), Map.of());
    }
    
    /**
     * Creates an unbounded RestSource with polling.
     */
    public static RestSource continuous(String baseUrl, int pageSize, Duration pollingInterval) {
        return new RestSource(baseUrl, pageSize, pollingInterval.toMillis(), false, Duration.ofSeconds(30), Map.of());
    }
    
    /**
     * Creates a RestSource with custom configuration.
     */
    public static RestSourceBuilder builder() {
        return new RestSourceBuilder();
    }
    
    private RestSource(
            String baseUrl,
            int pageSize, 
            long pollingIntervalMs,
            boolean isBounded,
            Duration requestTimeout,
            Map<String, String> headers) {
        
        this.baseUrl = baseUrl;
        this.pageSize = pageSize;
        this.pollingIntervalMs = pollingIntervalMs;
        this.isBounded = isBounded;
        this.requestTimeout = requestTimeout;
        this.headers = headers;
    }
    
    @Override
    public Boundedness getBoundedness() {
        return isBounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }
    
    @Override
    public SourceReader<String, RestSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RestRecord>> elementsQueue = 
            new FutureCompletingBlockingQueue<>();
        
        Supplier<SplitReader<RestRecord, RestSourceSplit>> splitReaderSupplier = () -> {
            RestApiClient apiClient = new RestApiClient(requestTimeout, headers);
            return new RestSplitReader(apiClient);
        };
        
        RecordEmitter<RestRecord, String, RestSourceSplitState> recordEmitter = new RestRecordEmitter();
        
        return new RestSourceReader(
            elementsQueue,
            splitReaderSupplier,
            recordEmitter,
            new Configuration(),
            readerContext
        );
    }
    
    @Override
    public SplitEnumerator<RestSourceSplit, RestSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<RestSourceSplit> enumContext) throws Exception {
        
        return new RestSourceEnumerator(
            enumContext,
            baseUrl,
            pageSize,
            pollingIntervalMs,
            isBounded,
            null // 초기 상태 없음
        );
    }
    
    @Override
    public SplitEnumerator<RestSourceSplit, RestSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<RestSourceSplit> enumContext,
            RestSourceEnumeratorState checkpoint) throws Exception {
        
        return new RestSourceEnumerator(
            enumContext,
            baseUrl,
            pageSize,
            pollingIntervalMs,
            isBounded,
            checkpoint
        );
    }
    
    @Override
    public SimpleVersionedSerializer<RestSourceSplit> getSplitSerializer() {
        return new RestSourceSplitSerializer();
    }
    
    @Override
    public SimpleVersionedSerializer<RestSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new RestSourceEnumeratorStateSerializer();
    }
    
    /**
     * Type information for the output type (String in this case).
     */
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
    
    /**
     * Builder class for RestSource.
     */
    public static class RestSourceBuilder {
        private String baseUrl;
        private int pageSize = 100;
        private Duration pollingInterval = Duration.ofMinutes(1);
        private boolean isBounded = true;
        private Duration requestTimeout = Duration.ofSeconds(30);
        private Map<String, String> headers = Map.of();
        
        public RestSourceBuilder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }
        
        public RestSourceBuilder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }
        
        public RestSourceBuilder pollingInterval(Duration pollingInterval) {
            this.pollingInterval = pollingInterval;
            this.isBounded = false; // 폴링이 설정되면 무제한으로 변경
            return this;
        }
        
        public RestSourceBuilder bounded() {
            this.isBounded = true;
            return this;
        }
        
        public RestSourceBuilder unbounded() {
            this.isBounded = false;
            return this;
        }
        
        public RestSourceBuilder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }
        
        public RestSourceBuilder headers(Map<String, String> headers) {
            this.headers = Map.copyOf(headers);
            return this;
        }
        
        public RestSource build() {
            if (baseUrl == null || baseUrl.trim().isEmpty()) {
                throw new IllegalArgumentException("baseUrl must not be null or empty");
            }
            
            return new RestSource(
                baseUrl,
                pageSize,
                pollingInterval.toMillis(),
                isBounded,
                requestTimeout,
                headers
            );
        }
    }
}
