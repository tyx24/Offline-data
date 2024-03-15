package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME


object Data_cleansing_3 {
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

    val ods_Table_URL = "hdfs://bigdata1:8020/user/hive/warehouse/ods_ds_hudi.db/base_province"

    val dwd_Table_URL = "hdfs://bigdata1:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_province"

    sparkSession.read.format("hudi").load(ods_Table_URL).createOrReplaceTempView("ods_v")
    val ods = sparkSession.sql("select * from ods_v where etl_date='20240401'")
      .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .drop("etl_date")

    val dwd = sparkSession.read.format("hudi").load(dwd_Table_URL)
      .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
      .drop("etl_date")

    dwd.show(false)

    ods.unionByName(dwd).createTempView("v")

    sparkSession.sql(
      """
        |select *,
        | row_number() over (partition by id order by create_time desc) as `create_time_num`,
        | min(dwd_insert_time) over (partition by id) as `min_insert_time`
        |from v
        |""".stripMargin)
      .withColumn("dwd_insert_time", when(col("create_time_num") === 1, col("min_insert_time"))
        .otherwise(col("dwd_insert_time")))
      .filter(col("create_time_num") === 1)
      .drop("create_time_num", "min_insert_time")
      .withColumn("etl_date", lit("20240401"))
      .write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(TBL_NAME.key(), "dim_province")
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