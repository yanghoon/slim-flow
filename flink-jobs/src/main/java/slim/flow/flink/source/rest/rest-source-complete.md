# Flink RestSource - 페이지 기반 REST API 소스 구현

## 개요

KafkaSource와 유사한 형태로 페이지 번호 기반으로 지속적으로 REST API를 호출하는 Flink Source를 FLIP-27 아키텍처로 구현했습니다.

## 주요 클래스와 역할

### 1. RestSourceSplit
```java
package com.example.flink.rest;

import org.apache.flink.api.connector.source.SourceSplit;
import java.io.Serializable;
import java.util.Objects;

/**
 * RestSourceSplit represents a page-based split for REST API calls.
 * Each split contains information about which page to fetch.
 */
public class RestSourceSplit implements SourceSplit, Serializable {
    
    private final String splitId;
    private final int pageNumber;
    private final int pageSize;
    private final String baseUrl;
    private final long lastProcessedTimestamp;
    
    public RestSourceSplit(String splitId, int pageNumber, int pageSize, String baseUrl) {
        this(splitId, pageNumber, pageSize, baseUrl, 0L);
    }
    
    public RestSourceSplit(String splitId, int pageNumber, int pageSize, String baseUrl, long lastProcessedTimestamp) {
        this.splitId = splitId;
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.baseUrl = baseUrl;
        this.lastProcessedTimestamp = lastProcessedTimestamp;
    }
    
    @Override
    public String splitId() {
        return splitId;
    }
    
    public int getPageNumber() {
        return pageNumber;
    }
    
    public int getPageSize() {
        return pageSize;
    }
    
    public String getBaseUrl() {
        return baseUrl;
    }
    
    public long getLastProcessedTimestamp() {
        return lastProcessedTimestamp;
    }
    
    public RestSourceSplit withNewPage(int newPageNumber) {
        return new RestSourceSplit(splitId, newPageNumber, pageSize, baseUrl, lastProcessedTimestamp);
    }
    
    public RestSourceSplit withTimestamp(long timestamp) {
        return new RestSourceSplit(splitId, pageNumber, pageSize, baseUrl, timestamp);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestSourceSplit that = (RestSourceSplit) o;
        return Objects.equals(splitId, that.splitId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
    
    @Override
    public String toString() {
        return "RestSourceSplit{" +
                "splitId='" + splitId + '\'' +
                ", pageNumber=" + pageNumber +
                ", pageSize=" + pageSize +
                ", baseUrl='" + baseUrl + '\'' +
                ", lastProcessedTimestamp=" + lastProcessedTimestamp +
                '}';
    }
}
```

### 2. RestSourceSplitSerializer
```java
package com.example.flink.rest;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;

/**
 * Serializer for RestSourceSplit.
 */
public class RestSourceSplitSerializer implements SimpleVersionedSerializer<RestSourceSplit> {
    
    private static final int VERSION = 1;
    
    @Override
    public int getVersion() {
        return VERSION;
    }
    
    @Override
    public byte[] serialize(RestSourceSplit split) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            
            oos.writeUTF(split.splitId());
            oos.writeInt(split.getPageNumber());
            oos.writeInt(split.getPageSize());
            oos.writeUTF(split.getBaseUrl());
            oos.writeLong(split.getLastProcessedTimestamp());
            
            oos.flush();
            return bos.toByteArray();
        }
    }
    
    @Override
    public RestSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            
            String splitId = ois.readUTF();
            int pageNumber = ois.readInt();
            int pageSize = ois.readInt();
            String baseUrl = ois.readUTF();
            long lastProcessedTimestamp = ois.readLong();
            
            return new RestSourceSplit(splitId, pageNumber, pageSize, baseUrl, lastProcessedTimestamp);
        }
    }
}
```

### 3. RestSourceEnumerator
```java
package com.example.flink.rest;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * SplitEnumerator for RestSource that manages page-based splits.
 * This enumerator creates splits for each page and assigns them to readers.
 */
public class RestSourceEnumerator implements SplitEnumerator<RestSourceSplit, RestSourceEnumeratorState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RestSourceEnumerator.class);
    
    private final SplitEnumeratorContext<RestSourceSplit> context;
    private final String baseUrl;
    private final int pageSize;
    private final long pollingIntervalMs;
    private final boolean isBounded;
    
    private final Queue<RestSourceSplit> pendingSplits;
    private final Set<String> assignedSplits;
    private final Set<Integer> registeredReaders;
    
    private int currentPageNumber;
    private boolean noMoreSplits;
    
    public RestSourceEnumerator(
            SplitEnumeratorContext<RestSourceSplit> context,
            String baseUrl,
            int pageSize,
            long pollingIntervalMs,
            boolean isBounded,
            @Nullable RestSourceEnumeratorState state) {
        
        this.context = context;
        this.baseUrl = baseUrl;
        this.pageSize = pageSize;
        this.pollingIntervalMs = pollingIntervalMs;
        this.isBounded = isBounded;
        
        this.pendingSplits = new ConcurrentLinkedQueue<>();
        this.assignedSplits = new HashSet<>();
        this.registeredReaders = new HashSet<>();
        
        if (state != null) {
            this.currentPageNumber = state.getCurrentPageNumber();
            this.noMoreSplits = state.isNoMoreSplits();
            this.pendingSplits.addAll(state.getPendingSplits());
            this.assignedSplits.addAll(state.getAssignedSplits());
        } else {
            this.currentPageNumber = 0; // 0부터 시작
            this.noMoreSplits = false;
        }
    }
    
    @Override
    public void start() {
        LOG.info("Starting RestSourceEnumerator with baseUrl: {}, pageSize: {}", baseUrl, pageSize);
        
        // 초기 Split 생성
        if (pendingSplits.isEmpty() && !noMoreSplits) {
            createInitialSplits();
        }
        
        // 무제한 소스의 경우 주기적으로 새로운 페이지 체크
        if (!isBounded && !noMoreSplits) {
            context.callAsync(
                this::checkForNewData,
                this::handleAsyncResult,
                0L,
                pollingIntervalMs
            );
        }
    }
    
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!registeredReaders.contains(subtaskId)) {
            LOG.warn("Received split request from unregistered reader {}", subtaskId);
            return;
        }
        
        assignSplits();
    }
    
    @Override
    public void addSplitsBack(List<RestSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        
        for (RestSourceSplit split : splits) {
            assignedSplits.remove(split.splitId());
            pendingSplits.offer(split);
        }
        
        assignSplits();
    }
    
    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {}", subtaskId);
        registeredReaders.add(subtaskId);
        assignSplits();
    }
    
    @Override
    public RestSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting state for checkpoint {}", checkpointId);
        
        return new RestSourceEnumeratorState(
            currentPageNumber,
            noMoreSplits,
            new ArrayList<>(pendingSplits),
            new HashSet<>(assignedSplits)
        );
    }
    
    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PageCompletedEvent) {
            PageCompletedEvent event = (PageCompletedEvent) sourceEvent;
            handlePageCompleted(event, subtaskId);
        }
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing RestSourceEnumerator");
    }
    
    private void createInitialSplits() {
        // 처음에 몇 개의 페이지 Split을 미리 생성
        int initialSplitsCount = Math.max(1, registeredReaders.size() * 2);
        
        for (int i = 0; i < initialSplitsCount; i++) {
            String splitId = generateSplitId(currentPageNumber);
            RestSourceSplit split = new RestSourceSplit(splitId, currentPageNumber, pageSize, baseUrl);
            pendingSplits.offer(split);
            currentPageNumber++;
        }
        
        LOG.info("Created {} initial splits", initialSplitsCount);
    }
    
    private void assignSplits() {
        Iterator<Integer> readerIterator = registeredReaders.iterator();
        
        while (!pendingSplits.isEmpty() && readerIterator.hasNext()) {
            RestSourceSplit split = pendingSplits.poll();
            int readerId = readerIterator.next();
            
            if (split != null) {
                context.assignSplit(split, readerId);
                assignedSplits.add(split.splitId());
                LOG.debug("Assigned split {} to reader {}", split.splitId(), readerId);
                
                // 순환 방식으로 다음 리더로 이동
                if (!readerIterator.hasNext()) {
                    readerIterator = registeredReaders.iterator();
                }
            }
        }
        
        // 모든 Split이 할당되었고 더 이상 Split이 없으면 완료 신호 전송
        if (pendingSplits.isEmpty() && assignedSplits.isEmpty() && (isBounded || noMoreSplits)) {
            for (int readerId : registeredReaders) {
                context.signalNoMoreSplits(readerId);
            }
        }
    }
    
    private void handlePageCompleted(PageCompletedEvent event, int subtaskId) {
        String completedSplitId = event.getSplitId();
        assignedSplits.remove(completedSplitId);
        
        boolean hasData = event.hasData();
        
        if (!isBounded && hasData) {
            // 무제한 소스이고 데이터가 있었다면 다음 페이지 Split 생성
            String nextSplitId = generateSplitId(currentPageNumber);
            RestSourceSplit nextSplit = new RestSourceSplit(nextSplitId, currentPageNumber, pageSize, baseUrl);
            pendingSplits.offer(nextSplit);
            currentPageNumber++;
        } else if (!hasData) {
            // 데이터가 없으면 더 이상 Split 생성 중지
            noMoreSplits = true;
        }
        
        assignSplits();
    }
    
    private Boolean checkForNewData() {
        // 실제 구현에서는 API를 호출하여 새로운 데이터가 있는지 확인
        // 여기서는 단순화하여 항상 true 반환
        return !noMoreSplits;
    }
    
    private void handleAsyncResult(Boolean hasNewData, Throwable throwable) {
        if (throwable != null) {
            LOG.error("Error checking for new data", throwable);
            return;
        }
        
        if (hasNewData && !noMoreSplits) {
            // 새로운 데이터가 있다면 다음 체크 스케줄링
            context.callAsync(
                this::checkForNewData,
                this::handleAsyncResult,
                pollingIntervalMs,
                pollingIntervalMs
            );
        }
    }
    
    private String generateSplitId(int pageNumber) {
        return String.format("rest-split-%d", pageNumber);
    }
}
```

### 4. RestApiClient (HTTP 클라이언트)
```java
package com.example.flink.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * HTTP client for making REST API calls with pagination support.
 */
public class RestApiClient {
    
    private static final Logger LOG = LoggerFactory.getLogger(RestApiClient.class);
    
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration requestTimeout;
    private final Map<String, String> headers;
    
    public RestApiClient() {
        this(Duration.ofSeconds(30), Map.of());
    }
    
    public RestApiClient(Duration requestTimeout, Map<String, String> headers) {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(requestTimeout)
            .build();
        this.objectMapper = new ObjectMapper();
        this.requestTimeout = requestTimeout;
        this.headers = headers;
    }
    
    /**
     * Fetches a page of data from the REST API.
     * 
     * @param baseUrl Base URL of the API
     * @param pageNumber Page number to fetch (0-based)
     * @param pageSize Number of records per page
     * @return RestApiResponse containing the data and metadata
     * @throws IOException if the request fails
     */
    public RestApiResponse fetchPage(String baseUrl, int pageNumber, int pageSize) throws IOException {
        String url = buildUrl(baseUrl, pageNumber, pageSize);
        
        LOG.debug("Fetching page {} with size {} from URL: {}", pageNumber, pageSize, url);
        
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(requestTimeout)
            .GET();
        
        // Add custom headers
        headers.forEach(requestBuilder::header);
        
        HttpRequest request = requestBuilder.build();
        
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200) {
                JsonNode jsonResponse = objectMapper.readTree(response.body());
                return parseResponse(jsonResponse, pageNumber, pageSize);
            } else {
                throw new IOException("HTTP request failed with status code: " + response.statusCode() + 
                                    ", body: " + response.body());
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request was interrupted", e);
        }
    }
    
    private String buildUrl(String baseUrl, int pageNumber, int pageSize) {
        // URL에 페이지 파라미터 추가
        // 실제 API에 따라 파라미터 이름이 다를 수 있음 (page, offset, start 등)
        String separator = baseUrl.contains("?") ? "&" : "?";
        return String.format("%s%spage=%d&size=%d", baseUrl, separator, pageNumber, pageSize);
    }
    
    private RestApiResponse parseResponse(JsonNode jsonResponse, int pageNumber, int pageSize) {
        // 일반적인 페이징 응답 구조를 가정
        // 실제 API 응답 구조에 맞게 수정 필요
        
        JsonNode dataNode = jsonResponse.get("data");
        JsonNode paginationNode = jsonResponse.get("pagination");
        
        if (dataNode == null) {
            // data 필드가 없으면 전체 응답을 데이터로 처리
            dataNode = jsonResponse;
        }
        
        boolean hasMore = true;
        int totalRecords = -1;
        
        if (paginationNode != null) {
            JsonNode hasMoreNode = paginationNode.get("hasMore");
            JsonNode totalNode = paginationNode.get("total");
            
            if (hasMoreNode != null) {
                hasMore = hasMoreNode.asBoolean();
            }
            if (totalNode != null) {
                totalRecords = totalNode.asInt();
                // total이 있으면 hasMore 계산
                hasMore = (pageNumber + 1) * pageSize < totalRecords;
            }
        } else {
            // pagination 정보가 없으면 데이터 크기로 판단
            if (dataNode.isArray()) {
                hasMore = dataNode.size() >= pageSize;
            } else {
                hasMore = false;
            }
        }
        
        return new RestApiResponse(dataNode, hasMore, pageNumber, pageSize, totalRecords);
    }
    
    public void close() {
        // HttpClient는 자동으로 리소스를 정리하므로 특별한 정리 작업 불필요
        LOG.debug("RestApiClient closed");
    }
}
```

### 5. 메인 RestSource 클래스
```java
package com.example.flink.rest;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

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
```

## 사용 예제

### 기본 사용법
```java
// 1. 간단한 bounded 소스
RestSource source = RestSource.bounded("https://api.example.com/data", 50);

DataStream<String> stream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "REST API Source"
);

// 2. 지속적인 폴링을 하는 unbounded 소스
RestSource continuousSource = RestSource.continuous(
    "https://api.example.com/data",
    100,
    Duration.ofMinutes(5) // 5분마다 폴링
);

// 3. 커스텀 설정
RestSource customSource = RestSource.builder()
    .baseUrl("https://api.example.com/secure-data")
    .pageSize(25)
    .pollingInterval(Duration.ofSeconds(30))
    .requestTimeout(Duration.ofSeconds(60))
    .headers(Map.of(
        "Authorization", "Bearer your-token-here",
        "Content-Type", "application/json"
    ))
    .build();
```

## 클래스 생성 시점과 호출 시점

### 1. Source 초기화
- **시점**: Job 제출 시
- **위치**: Client에서 JobGraph 생성 시
- **역할**: 모든 컴포넌트의 팩토리 역할

### 2. SplitEnumerator 생성
- **시점**: JobManager에서 SourceCoordinator 시작 시
- **위치**: JobManager (단일 인스턴스)
- **생성**: `RestSource.createEnumerator()` 또는 `restoreEnumerator()` 호출
- **복구**: 체크포인트에서 `RestSourceEnumeratorState` 복원

### 3. SourceReader 생성  
- **시점**: TaskManager에서 SourceOperator 시작 시
- **위치**: 각 TaskManager (병렬도만큼 생성)
- **생성**: `RestSource.createReader()` 호출

### 4. Split 생성과 할당 과정
- **초기 생성**: `RestSourceEnumerator.start()` → `createInitialSplits()`
- **Split 요청**: `SourceReader.start()` → `context.sendSplitRequest()`
- **Split 할당**: `SplitEnumerator.handleSplitRequest()` → `context.assignSplit()`
- **Split 수신**: `SourceReader.addSplits()`

### 5. 데이터 읽기 과정
- **메인 루프**: `SourceReader.pollNext()` → `RestSplitReader.fetch()`
- **API 호출**: `RestApiClient.fetchPage()` → HTTP 요청
- **완료 통보**: `PageCompletedEvent` 전송

## 주요 특징

1. **KafkaSource 유사 구조**: 파티션 대신 페이지를 Split으로 처리
2. **페이지 기반 분산**: 각 페이지가 하나의 Split으로 처리되어 병렬 처리 가능
3. **Bounded/Unbounded 지원**: 한 번만 읽기 또는 지속적 폴링 지원
4. **체크포인트 지원**: Split 상태와 Enumerator 상태 완전 복구 가능
5. **유연한 설정**: 헤더, 타임아웃, 폴링 간격 등 커스터마이징 가능
6. **에러 핸들링**: HTTP 에러, 네트워크 오류 등에 대한 적절한 처리

이 구현을 통해 다양한 REST API와 연동하여 페이지 기반으로 데이터를 효율적으로 읽어올 수 있습니다.