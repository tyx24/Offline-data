package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Data_cleansing_5 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val Conf = new SparkConf().setMaster("local[*]").setAppName("Data_cleansing_5")

    val sparkSession = SparkSession.builder().enableHiveSupport().config(Conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    //获取ods数据
    val ods = sparkSession.sql("select * from ods.order_info where etl_date='20240401'")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .drop("etl_date")
      .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))

    ods.withColumn("operate_time", when(col("operate_time").isNull, col("create_time"))
      .otherwise(col("operate_time"))).createTempView("result_v")

    sparkSession.sql("insert overwrite table dwd.fact_order_info select * from result_v")

    sparkSession.stop()

  }
}
