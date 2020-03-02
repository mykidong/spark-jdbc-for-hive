package mykidong.datasources;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkLoader {

    public static SparkConf getDefaultConf(String appName)
    {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appName);

        return sparkConf;
    }

    public static SparkConf addESConf(SparkConf sparkConf, String esHost, String esPort)
    {
        sparkConf.set("es.nodes", esHost)
                .set("es.port", esPort)
                .set("es.index.read.missing.as.empty", "true")
                .set("es.modes.wan.only", "true");

        return sparkConf;
    }


    public static SparkConf getDefaultLocalConf(String appName, int threadCount)
    {
        // spark configuration for local mode.
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        sparkConf.setMaster("local[" + threadCount + "]");
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy");


        return sparkConf;
    }

    public static SparkSession getSessionWithHadoopProperties(SparkSession spark, Properties hadoopProps)
    {
        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

        for (String key : hadoopProps.stringPropertyNames()) {
            String value = hadoopProps.getProperty(key);
            hadoopConfiguration.set(key, value);
        }

        return spark;
    }
}
