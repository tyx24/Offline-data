package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}

import java.util.Properties

object Data_extraction_4 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.ERROR)

    //warehouse
    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val Conf = new SparkConf().setMaster("local[*]").setAppName("Data_extraction_4")

    val sparkSession = SparkSession.builder().enableHiveSupport().config(Conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .getOrCreate()

    val jdbcURL = "jdbc:mysql://bigdata1:3306/shtd_store"
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "123456")

    sparkSession.read.jdbc(jdbcURL, "base_region", properties).withColumn("create_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .createTempView("v")

    val maxid = sparkSession.sql(
      """
        |select max(id) from ods.base_region
        |""".stripMargin).collect()(0).get(0).toString.toInt

    sparkSession.sql(
      s"""
         |insert into table ods.base_region partition(etl_date="20240401")
         |select * from v
         |where id > $maxid
         |""".stripMargin)

    sparkSession.stop()
  }
}
