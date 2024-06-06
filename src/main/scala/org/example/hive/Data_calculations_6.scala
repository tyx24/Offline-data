package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_6 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.ERROR)

    //warehouse
    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val Conf = new SparkConf().setMaster("local[*]").setAppName("Data_calculations_5")

    val sparkSession = SparkSession.builder().enableHiveSupport().config(Conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val MysqlURL = "jdbc:mysql://bigdata1:3306/shtd_result?useUnicode=true&characterEncoding=UTF-8"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")

    val dataFrame = sparkSession.sql(
      """
        |select
        |regionid,
        |regionname,
        |concat_ws(",",collect_list(cast(provinceid as STRING))) as provinceids,
        |concat_ws(",",collect_list(provincename)) as provincenames,
        |concat_ws(",",collect_list(cast(province_total_amount as STRING))) as provinceamount
        |FROM
        |(
        | SELECT
        | region_id regionid,
        | region_name regionname,
        | province_id provinceid,
        | province_name provincename,
        | sum(`total_amount`) as `province_total_amount`,
        | row_number() over (partition by region_id order by sum(`total_amount`) desc) as `num`
        | FROM dws.province_consumption_day_aggr
        | where year=2020
        | group by region_id,region_name,
        |          province_id,province_name
        | )
        | where `num`<=3
        | group by regionid,regionname
        |""".stripMargin)

    dataFrame.write.mode("overwrite").jdbc(MysqlURL, "regiontopthree", properties)

    sparkSession.read.jdbc(MysqlURL, "regiontopthree", properties).show()

    sparkSession.stop()
  }
}
