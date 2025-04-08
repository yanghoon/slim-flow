package slim.flow.flink.job;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see https://thej3.com/apache-iceberg-in-action-with-apache-flink-using-java-158500688ead
 */
public class IcebergSourceJob {

    private static final Logger log = LoggerFactory.getLogger(IcebergSourceJob.class);

    public static void main(String[] args) throws Exception {
        // TODO: Remove from Code
        var configDir = "flink-jobs/src/main/resources/conf/";
        var conf = GlobalConfiguration.loadConfiguration(configDir);
        // conf.set(CommonCatalogOptions.CATALOG_TYPE, "iceberg");
        // conf.setString(CatalogProperties.CATALOG_IMPL, RESTCatalog.class.getName());
        // conf.setString(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_REST);

        System.setProperty("aws.region", "us-east-1");

        log.info("Configuration is loaded.\n{}", conf);

        // Env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createCatalog("iceberg", CatalogDescriptor.of("iceberg", conf));
        tableEnv.useCatalog("iceberg");

        // Source
        // var table = tableEnv.sqlQuery("SELECT * FROM `default`.test ORDER BY id"); //Err: Sort on a non-time-attribute field is not supported.
        var table = tableEnv.sqlQuery("SELECT * FROM `default`.test");

        // Stream
        var stream = tableEnv.toDataStream(table);

        // Sink
        stream.print();

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
