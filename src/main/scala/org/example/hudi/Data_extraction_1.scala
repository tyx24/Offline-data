package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME

import java.util.Properties

object Data_extraction_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val conf = new SparkConf().setMaster("local[*]").setAppName("Data_extraction_1")

    val sparkSession = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .getOrCreate()

    val TableURL = "hdfs://bigdata1:8020/user/hive/warehouse/ods_ds_hudi.db/user_info"

    val jdbcURL = "jdbc:mysql://bigdata1:3306/shtd_store?useUnicode=true&characterEncoding=UTF-8"
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "123456")

    sparkSession.read.jdbc(jdbcURL, "user_info", properties)
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time")))
      .createTempView("v")

    val data = sparkSession.read.format("hudi").load(TableURL)

    data.createOrReplaceTempView("hudi_table_v")

    val max_time = sparkSession.sql("select if(max(operate_time)>max(create_time),max(operate_time),max(create_time)) from hudi_table_v")
      .collect()(0).get(0).toString

    sparkSession.sql(
      s"""
         |select
         |*
         |from v
         |where operate_time > cast('$max_time' as timestamp) or create_time > cast('$max_time' as timestamp)
         |""".stripMargin)
      .withColumn("etl_date", lit("20240401"))
      .write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(TBL_NAME.key(), "user_info")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "operate_time")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "etl_date")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key(), "true")
      .mode(SaveMode.Append)
      .save(TableURL)

    sparkSession.read.format("hudi").load(TableURL).show()

    sparkSession.stop()
  }
}
