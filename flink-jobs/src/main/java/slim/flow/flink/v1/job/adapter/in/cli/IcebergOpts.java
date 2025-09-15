package slim.flow.flink.v1.job.adapter.in.cli;

import java.lang.module.Configuration;

import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Command;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Option;

@Command
public class IcebergOpts {

    @Option(
        names = {"-i", "--iceberg-config"},
        description = "The Iceberg REST catalog URL",
        required = false)
    private Map<String, String> configMap;
    
    @Option(
        names = {"-t", "--table"},
        description = "The Iceberg table to write to, in the format 'database.table'",
        required = true)
    private String table; 

    @Option(
        names = {"--s3-endpoint"},
        defaultValue = "${S3_ENDPOINT:http://localhost:9000}",
        description = "The S3 endpoint URL",
        required = true)
    private String endpoint;

    @Option(
        names = {"--s3-access-key"},
        defaultValue = "${S3_ACCESS_KEY:minio}",
        description = "The S3 access key",
        required = false)
    private String accessKey;

    @Option(
        names = {"--s3-secret-key"},
        defaultValue = "${S3_SECRET_KEY:miniosecret}",
        description = "The S3 secret key",
        required = false)
    private String secretKey;

    public Configuration config() {
        var defaultConfig = loadDefaultConfig();

        configMap.forEach(defaultConfig::setString);

        defaultConfig.setString("table", table);
        
        defaultConfig.setString("s3.endpoint", endpoint);
        defaultConfig.setString("s3.access-key-id", accessKey);
        defaultConfig.setString("s3.secret-access-key", secretKey);

        return defaultConfig;
    }

    private Configuration loadDefaultConfig() {
        var configPath = "config/config.yaml";
        var tmpPath = Paths.get(System.getProperty("java.io.tmpdir"), "flink-config/config.yaml");

        try (var in = getClass().getClassLoader().getResourceAsStream(configPath)) {
            tmpPath.toFile().getParentFile().mkdirs();

            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);

            return GlobalConfiguration.loadConfiguration(tmpPath.getParent().toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load default Flink Iceberg config", e);
        }
    }

    public static IcebergOpts parse(String[] args) {
        var opts = new IcebergOpts();
        new CommandLine(opts).setUnmatchedArgumentsAllowed(true).parse(args);
        return opts;
    }

}
