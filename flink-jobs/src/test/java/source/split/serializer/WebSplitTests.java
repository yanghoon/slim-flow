package source.split.serializer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.Test;

import slim.flow.flink.source.web.WebSplit;

public class WebSplitTests {

    @Test
    public void test_web_split_serializer() throws IOException {
        var original = WebSplit.builder().build();

        var serializer = new WebSplit.Serializer();
        var bytes = serializer.serialize(original);
        var restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(WebSplit.class, restored.getClass(), "splitId should be the same");
        assertEquals(original.getClass(), restored.getClass(), "splitId should be the same");
        assertEquals(original.splitId(), restored.splitId(), "splitId should be the same");

    }
    
}
