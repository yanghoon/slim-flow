package slim.flow.flink.source.websocket;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebSocketClient {

    // private WebSocket webSocket;

    public <T> void connect(String url, Map<String, String> query, Consumer<T> handler) {
        try {
            // Build the full URL with query parameters
            String fullUrl = buildUrlWithQuery(url, query);

            // Create an HttpClient
            HttpClient client = HttpClient.newHttpClient();

            // Build the WebSocket
            // webSocket = client.newWebSocketBuilder()
            client.newWebSocketBuilder()
                    .buildAsync(URI.create(fullUrl), new WebSocketListener<>(handler))
                    .join();

            log.info("Connected to WebSocket server at: {}", fullUrl);

        } catch (Exception e) {
            log.error("Error during WebSocket connection: {}", e.getMessage(), e);
        }
    }

    private String buildUrlWithQuery(String url, Map<String, String> query) {
        if (query == null || query.isEmpty()) {
            return url;
        }
        StringBuilder fullUrl = new StringBuilder(url);
        fullUrl.append("?");
        query.forEach((key, value) -> fullUrl.append(key).append("=").append(value).append("&"));
        fullUrl.deleteCharAt(fullUrl.length() - 1); // Remove the trailing '&'
        return fullUrl.toString();
    }

    private static class WebSocketListener<T> implements Listener {
        private final Consumer<T> handler;

        public WebSocketListener(Consumer<T> handler) {
            this.handler = handler;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            Listener.super.onOpen(webSocket);
            log.info("WebSocket connection opened.");
            webSocket.request(1); // Request the first message
            webSocket.sendText("Hello", false); // Request the first message
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            log.info("Received message: {}", data);
            try {
                handler.accept((T) data);
                // log.info("Handler executed with result: {}", result);
            } catch (Exception e) {
                log.error("Error executing handler: {}", e.getMessage(), e);
            }
            webSocket.request(1); // Request the next message
            return Listener.super.onText(webSocket, data, last);
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            log.warn("Binary messages are not supported.");
            webSocket.request(1);
            return Listener.super.onBinary(webSocket, data, last);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.info("WebSocket connection closed. Code: {}, Reason: {}", statusCode, reason);
            return Listener.super.onClose(webSocket, statusCode, reason);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.error("WebSocket error: {}", error.getMessage(), error);
        }
    }

}
