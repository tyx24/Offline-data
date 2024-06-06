package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME


object Data_cleansing_6 {
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

    val ods_Table_URL = "hdfs://bigdata1:8020/user/hive/warehouse/ods_ds_hudi.db/order_detail"

    val dwd_Table_URL = "hdfs://bigdata1:8020/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail"

    sparkSession.read.format("hudi").load(ods_Table_URL).createOrReplaceTempView("ods_v")

    sparkSession.sql("select * from ods_v where etl_date='20240401'")
      .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .drop("etl_date")
      .createOrReplaceTempView("ods_v")

    sparkSession.sql(
      """
        |select *
        |from ods_v
        |""".stripMargin)
      .withColumn("etl_date", lit(date_format(col("create_time"), "yyyyMMdd")))
      .write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(TBL_NAME.key(), "fact_order_detail")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "dwd_modify_time")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "etl_date")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key(), "true")
      .mode(SaveMode.Append)
      .save(dwd_Table_URL)

    sparkSession.read.format("hudi").load(dwd_Table_URL).show()

    sparkSession.stop()
  }
}