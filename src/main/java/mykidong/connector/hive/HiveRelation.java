package mykidong.connector.hive;

import mykidong.datasources.jdbc.hive.JdbcHiveOptions;
import mykidong.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

public class HiveRelation extends BaseRelation implements Serializable, TableScan {

    private SQLContext sqlContext;
    private StructType schema;
    private java.util.Map<String, String> parametersAsJava;

    private Dataset<Row> df;

    public HiveRelation(SQLContext sqlContext, Map<String, String> parameters)
    {
        this.sqlContext = sqlContext;
        this.parametersAsJava = mapAsJavaMapConverter(parameters).asJava();
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        if(schema == null)
        {
            execHiveQuery();
        }

        return this.schema;
    }

    private void execHiveQuery()
    {
        String hiveJdbcUrl = parametersAsJava.get(HiveOptions.hiveJdbcUrl);
        String hiveJdbcUser = parametersAsJava.get(HiveOptions.hiveJdbcUser);
        String hiveJdbcPassword = parametersAsJava.get(HiveOptions.hiveJdbcPassword);
        String query = parametersAsJava.get(HiveOptions.query);
        String defaultFs = parametersAsJava.get(HiveOptions.defaultFs);
        String hadoopConfProperties = parametersAsJava.get(HiveOptions.hadoopConfProperties);
        String outputPath = parametersAsJava.get(HiveOptions.outputPath);

        outputPath = outputPath + "/" + UUID.randomUUID().toString() + "-" + UUID.randomUUID().toString();
        System.out.println("outputPath: [" + outputPath + "]");

        String sql = StringUtils.fileToString("/hive-template/hive-query.sql");
        sql = sql.replaceAll("#query#", query);
        sql = sql.replaceAll("#outputPath#", outputPath);
        System.out.println("sql: [" + sql + "]");
        try {
            // spark hadoop configuration 을 hive 가 query 실행후 저장하는 file system 으로 변경.
            if(hadoopConfProperties != null) {
                // hadoop configuration.
                Resource resource = new ClassPathResource(hadoopConfProperties);
                Properties hadoopProps = PropertiesLoaderUtils.loadProperties(resource);

                alterHadoopConfiguration(sqlContext.sparkSession(), hadoopProps);
            } else {
                Configuration hadoopConfiguration = sqlContext.sparkContext().hadoopConfiguration();
                hadoopConfiguration.set("fs.defaultFS", defaultFs);
            }

            Connection conn = DriverManager.getConnection(hiveJdbcUrl, hiveJdbcUser, hiveJdbcPassword);
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql);

            statement.close();
            if(conn != null)
            {
                conn.close();
            }

            // hive 가 저장한 path 에서 parquet file 을 읽음.
            df = sqlContext.read().format("parquet").load(outputPath);
            this.schema = df.schema();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void alterHadoopConfiguration(SparkSession spark, Properties hadoopProps)
    {
        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

        for (String key : hadoopProps.stringPropertyNames()) {
            String value = hadoopProps.getProperty(key);
            hadoopConfiguration.set(key, value);
        }
    }

    @Override
    public RDD<Row> buildScan() {
        if(df == null)
        {
            execHiveQuery();
        }

        return df.rdd();
    }
}
