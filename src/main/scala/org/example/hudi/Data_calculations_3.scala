package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val conf = new SparkConf().setMaster("local[*]").setAppName("Data_calculations_3")

    val sparkSession = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()


    val dataFrame = sparkSession.sql(
      """
        |SELECT *,
        |  CASE
        |    WHEN provinceavgconsumption > regionavgconsumption THEN '高'
        |    WHEN provinceavgconsumption < regionavgconsumption THEN '低'
        |    ELSE '相同'
        |  END AS comparison
        |FROM (
        |  SELECT
        |    province_id AS provinceid,
        |    province_name AS provincename,
        |    total_amount / total_count AS provinceavgconsumption,
        |    region_id AS region_id,
        |    region_name AS region_name,
        |    SUM(total_amount) OVER (PARTITION BY year, month, region_name)/ SUM(total_count) OVER (PARTITION BY year, month, region_name) AS regionavgconsumption
        |  FROM dws_ds_hudi.province_consumption_day_aggr
        |  WHERE year = 2020 AND month = 4
        |)
        |""".stripMargin)


    val properties = new Properties()
    properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    properties.put("user", "default")
    properties.put("password", "123456")
    properties.put("batchsize", "100000")

    val URL = "jdbc:clickhouse://bigdata1:8123/shtd_result"

    //插入数据
    dataFrame.write.mode("append").jdbc(URL, "provinceavgcmpregion", properties)

    sparkSession.stop()
  }
}
