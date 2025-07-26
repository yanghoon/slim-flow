package slim.flow.flink.job;

import java.util.Optional;

public class CsvFileSourceJob {

    public static void main(String[] args) throws Exception {
        fromClasspath("users.csv");
    }

    private static void fromClasspath(String path) {
        // TODO: check null
        var loader = ClassLoader.getSystemClassLoader();
        var resource = Optional.ofNullable(loader.getResource(path));

        System.out.println(resource);
    }

}
