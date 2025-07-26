package slim.flow.flink.job;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkJob {

    private static final Logger log = LoggerFactory.getLogger(IcebergSinkJob.class);

    public static void main(String[] args) throws Exception {
        // TODO: Remove from Code
        var configDir = "flink-jobs/src/main/resources/conf/";
        var conf = GlobalConfiguration.loadConfiguration(configDir);
        System.setProperty("aws.region", "us-east-1");

        log.info("Configuration is loaded.\n{}", conf);

        // Env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var catalog = CatalogLoader.rest("iceberg", new Configuration(), conf.toMap());
        var table = TableLoader.fromCatalog(catalog, TableIdentifier.of("default", "test"));

        // Source
        var source = env.fromData(
            rowData(10, "admin", 39),
            rowData(20, "user1", 27),
            rowData(30, "user2", 26)
        );

        // Sink
        FlinkSink.forRowData(source)
            .tableLoader(table)
            .append();

        source.print();

        // Execute
        env.execute();
    }

    private static RowData rowData(int id, String name, int age) {
        var rowData = new GenericRowData(3);
        rowData.setField(0, id);
        rowData.setField(1, StringData.fromString(name));
        rowData.setField(2, age);
        return rowData;
    }
    
}
