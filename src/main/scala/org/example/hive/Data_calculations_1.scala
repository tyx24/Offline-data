package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_1 {
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
      .getOrCreate()

    val MysqlURL = "jdbc:mysql://bigdata1:3306/shtd_result?useUnicode=true&characterEncoding=UTF-8"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")


    val dataFrame = sparkSession.sql(
      """
        |select
        |province.id provinceid,
        |province.name provincename,
        |region.id regionid,
        |region.region_name regionname,
        |sum(`final_total_amount`) totalconsumption,
        |count(1) totalorder,
        |year(order.create_time) year,
        |month(order.create_time) month
        |from dwd.fact_order_info order
        | join dwd.dim_province province
        | on (order.province_id = province.id
        |      and province.etl_date='20240401')
        |  join dwd.dim_region region
        |   on (province.region_id = region.id
        |       and region.etl_date='20240401'
        |       and province.etl_date='20240401')
        | group by province.id,province.name,
        |         region.id,region.region_name,
        |         year(order.create_time),
        |         month(order.create_time)
        |""".stripMargin)

    dataFrame.write.mode("overwrite").jdbc(MysqlURL, "provinceeverymonth", properties)

    sparkSession.read.jdbc(MysqlURL, "provinceeverymonth", properties).show()

    sparkSession.stop()
  }
}
