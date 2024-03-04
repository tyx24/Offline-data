package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Data_calculations_4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.ERROR)

    //warehouse
    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val Conf = new SparkConf().setMaster("local[*]").setAppName("Data_cleansing_1")

    val sparkSession = SparkSession.builder().enableHiveSupport().config(Conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()


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
        | FROM dwd.fact_order_info order
        | JOIN dwd.dim_province province
        |   ON (order.province_id = province.id
        |      and province.etl_date='20240401')
        | JOIN dwd.dim_region region
        |   ON (province.region_id = region.id
        |       and region.etl_date='20240401'
        |       and province.etl_date='20240401')
        | GROUP BY province.id,province.name,
        |         region.id,region.region_name,
        |         year(order.create_time),
        |         month(order.create_time)
        | )
        |""".stripMargin)

    dataFrame.show()
    //    dataFrame.createTempView("result_v")
    //
    //    sparkSession.sql("INSERT OVERWRITE TABLE dws.province_consumption_day_aggr PARTITION (year, month) SELECT * FROM result_v")
    //
    //    sparkSession.sql("select * from dws.province_consumption_day_aggr").show()

    sparkSession.stop()
  }
}
