package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.DataSourceWriteOptions

import java.util.Properties

object Data_extraction_4 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Data_extraction_4")

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val sparkSession = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .getOrCreate()

    val Table_ULR = "hdfs://bigdata1:8020/user/hive/warehouse/ods_ds_hudi.db/base_region"

    val Mysql_URL = "jdbc:mysql://bigdata1:3306/shtd_store?useUnicode=true&characterEncoding=UTF-8"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("Driver", "com.mysql.jdbc.Driver")

    sparkSession.read.jdbc(Mysql_URL, "base_region", properties)
      .withColumn("create_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .createOrReplaceTempView("v")


    sparkSession.read.format("hudi").load(Table_ULR).createOrReplaceTempView("hudi_table_v")

    sparkSession.read.format("hudi").load(Table_ULR).show()

    val max_id = sparkSession.sql("select max(id) from hudi_table_v").collect()(0).get(0).toString.toInt

    sparkSession.sql(
      s"""
         |select
         |*
         |from v
         |where id>${max_id}
         |""".stripMargin)
      .withColumn("etl_date", lit("20240401"))
      .write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(TBL_NAME.key(), "base_region")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "create_time")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "etl_date")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key(), "true")
      .mode(SaveMode.Append)
      .save(Table_ULR)

    sparkSession.read.format("hudi").load(Table_ULR).show()

    sparkSession.stop()
  }
}
