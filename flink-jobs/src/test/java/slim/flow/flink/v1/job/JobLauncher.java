package slim.flow.flink.v1.job;

public class JobLauncher {
    
    public static void main(String[] args) {
        launchWebPipeline();
        // launchIcebergPrint();
    }

    private static launchWebPipeline() {
        var args = new String[] {
            "--url", "https://jsonplaceholder.typicode.com/posts",
            "--query", "_limit=2",
            "--query", "_page=1",
            // "--iceberg-config", "uri=http://localhost:8181/",
            // "--s3-endpoint", "http://localhost:9000",
            // "--s3-access-key", "minio",
            // "--s3-secret-key", "miniosecret",
            "--table", "default.post",
        };
        WebPipelineJob.main(args);
    }

    private static launchIcebergPrint() {
        var args = new String[] {
            // "--iceberg-config", "uri=http://localhost:8181/",
            // "--s3-endpoint", "http://localhost:9000",
            // "--s3-access-key", "minio",
            // "--s3-secret-key", "miniosecret",
            "--table", "default.post",
        };
        IcebergTablePrintJob.main(args);
    }

}