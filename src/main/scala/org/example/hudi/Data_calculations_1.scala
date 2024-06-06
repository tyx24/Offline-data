package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME


object Data_calculations_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val conf = new SparkConf().setMaster("local[*]").setAppName("Data_extraction_1")

    val sparkSession = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql(
      """
        |create table if not exists dws_ds_hudi.user_consumption_day_aggr
        |(
        |    `uuid`         String,
        |    `user_id`      INT,
        |    `user_name`    string,
        |    `total_amount` double,
        |    `total_count`  int
        |)using hudi
        |    options(
        |        type = 'mor',
        |        primaryKey = 'uuid',
        |        preCombineField = 'total_count'
        |    )
        |    partitioned by (`year` Int,`month` Int,`day` Int);
        |""".stripMargin)

    val dws_Table_URL = "hdfs://bigdata1:8020/user/hive/warehouse/dws_ds_hudi.db/user_consumption_day_aggr"

    val dataFrame = sparkSession.sql(
      s"""
         |select `user_id`,
         |       `name`                             as user_name,
         |       sum(`final_total_amount`)          as total_amount,
         |       count(*)                           as `total_count`,
         |       year(dim_user_info.`create_time`)  as `year`,
         |       month(dim_user_info.`create_time`) as `month`,
         |       day(dim_user_info.`create_time`)   as `day`
         |  from dwd_ds_hudi.fact_order_info
         |         join dwd_ds_hudi.dim_user_info
         |          on (dim_user_info.id = user_id
         |              and dim_user_info.etl_date = '20240401')
         |group by `user_id`,
         |         `name`,
         |         year(dim_user_info.`create_time`),
         |         month(dim_user_info.`create_time`),
         |         day(dim_user_info.`create_time`)
         |""".stripMargin)
      .withColumn("uuid", expr("uuid()"))

    dataFrame.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(TBL_NAME.key(), "user_consumption_day_aggr")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "uuid")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "total_count")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "year,month,day")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key(), "true")
      .mode(SaveMode.Overwrite)
      .save(dws_Table_URL)

    sparkSession.read.format("hudi").load(dws_Table_URL).show()

    sparkSession.stop()

  }
}