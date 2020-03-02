package mykidong.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;

public class HiveConnectorTestSkip {

    @Test
    public void loadDataFromHive() throws Exception
    {
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("windows")) {
            System.setProperty("hadoop.home.dir", "C:\\hadoop-home");
        }

        SparkConf sparkConf = new SparkConf().setAppName("hive-jdbc");
        sparkConf.setMaster("local[2]");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        // hadoop configuration.
        Resource resource = new ClassPathResource("hadoop-conf.properties");
        Properties hadoopProps = PropertiesLoaderUtils.loadProperties(resource);

        // set hadoop configuration to the current spark session.
        spark = SparkLoader.getSessionWithHadoopProperties(spark, hadoopProps);

        Dataset<Row> hiveDf = spark.read().format("hive-with-jdbc")
                .option("dbTable", "mc.crawl_youtube")
                .option("hiveJdbcUrl", "jdbc:hive2://mc-d01.mykidong.io:10000")
                .option("hiveJdbcUser", "xxxx")
                .option("hiveJdbcPassword", "xxxx")
                .option("hiveMetastoreUrl", "jdbc:mysql://mc-d01.mykidong.io:3306/hive")
                .option("hiveMetastoreUser", "xxxx")
                .option("hiveMetastorePassword", "xxxx")
                .option("query", "SELECT * FROM mc.crawl_youtube where year = '2020' and month = '02' and day = '19'")
                .option("defaultFs", "")
                .option("hadoopConfProperties", "hadoop-conf.properties")
                .option("outputPath", "/temp-hive")
                .load();

        hiveDf.show(100);

        hiveDf.printSchema();
    }
}
