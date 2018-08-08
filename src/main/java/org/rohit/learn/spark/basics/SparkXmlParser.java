/**
 * Created by rramwal on 08/08/18.
 */

package org.rohit.learn.spark.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class SparkXmlParser {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("xmlparser").setMaster("local[*]").set("spark.driver.host", "localhost");
        SQLContext sqlContext = new SQLContext(SparkContext.getOrCreate(sparkConf));
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.xml").option("rowTag", "breakfast_menu").option("rowTag", "food").load("/Users/rramwal/gitdev/SparkXmlParser/src/main/resources/inputfile.xml");
        df.repartition(1).write().mode("overwrite").json("/Users/rramwal/gitdev/SparkXmlParser/src/main/resources/outfile1");
    }
}
