package slim.flow.flink.v1.job.iceberg;

import slim.flow.flink.v1.job.adapter.in.cli.IcebergOpts;

public class JobLauncher {
    
    public static void main(String[] args) {
        var env = StreamEnvironment.getExecutionEnvironment();
        var tableEnv = TableEnvironment.create(env);

        var catalogConfig = IcebergOpts.parse(args).config();
        var catalog = CatalogDescripter.of("iceberg", catalogConfig);
        tableEnv.createCatalog("iceberg", catalog);
        tableEnv.useCatalog("iceberg");

        var sourceTable = catalogConfig.getString("table", "default.test");
        // var icebergTable = tableEnv.loadTable(TableIdentifier.parse(table));
        var sql = "SELECT * FROM " + sourceTable;
        var table = tableEnv.sqlQuery(sql);
        var stream = tableEnv.toDataStream(table);
        stream.print("Iceberg Row ");

        env.execute("Iceberg Table Print Job");
    }

}