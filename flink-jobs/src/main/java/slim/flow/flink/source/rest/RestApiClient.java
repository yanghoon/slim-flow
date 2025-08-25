package slim.flow.flink.source.rest;

package com.example.flink.rest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
