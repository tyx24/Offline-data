package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs

object Data_calculations_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val conf = new SparkConf().setMaster("local[*]").setAppName("Data_calculations_2")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql(
      """
        |create table if not exists dws_ds_hudi.province_consumption_day_aggr
        |(
        |    `uuid`          String,
        |    `province_id`   INT,
        |    `province_name` string,
        |    `region_id`     INT,
        |    `region_name`   STRING,
        |    `total_amount`  double,
        |    `total_count`   int,
        |    sequence        INT
        |)using hudi
        |    options(
        |        type = 'mor',
        |        primaryKey = 'uuid',
        |        preCombineField = 'total_count'
        |    )
        |    partitioned by (`year` Int,`month` Int);
        |""".stripMargin)

    val dws_Table_URL = "hdfs://bigdata1:8020/user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr"

    val dataFrame = sparkSession.sql(
      """
        |SELECT
        |province_id,
        |province_name,
        |region_id,
        |region_name,
        |total_amount,
        |total_count,
        |row_number() over (partition by `year`,`month`,`region_id` order by total_amount desc) as sequence,
        |`year`,
        |`month`
        |FROM
        | (SELECT
        |   province.id province_id,
        |   province.name province_name,
        |   region.id region_id,
        |   region.region_name region_name,
        |   sum(`final_total_amount`) total_amount,
        |   count(*) total_count,
        |   year(order.create_time) as `year`,
        |   month(order.create_time) as `month`
        | FROM dwd_ds_hudi.fact_order_info order
        | JOIN dwd_ds_hudi.dim_province province
        |   ON (order.province_id = province.id
        |      and province.etl_date='20240401')
        | JOIN dwd_ds_hudi.dim_region region
        |   ON (province.region_id = region.id
        |       and region.etl_date='20240401'
        |       and province.etl_date='20240401')
        | GROUP BY province.id,province.name,
        |         region.id,region.region_name,
        |         year(order.create_time),
        |         month(order.create_time)
        | )
        |""".stripMargin)

    dataFrame
      .withColumn("uuid", expr("uuid()"))
      .write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(TBL_NAME.key(), "province_consumption_day_aggr")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "uuid")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "total_count")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "year,month")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key(), "true")
      .option(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key(), "true")
      .mode(SaveMode.Overwrite)
      .save(dws_Table_URL)


    sparkSession.sql(
      """
        |select uuid,
        |       province_id,
        |       province_name,
        |       region_id,
        |       region_name,
        |       total_count,
        |       sequence,
        |       year,
        |       month
        |   from dws_ds_hudi.province_consumption_day_aggr
        |order by total_count desc, total_amount desc, province_id desc
        |limit 5;
        |""".stripMargin).show(false)

  }
}
