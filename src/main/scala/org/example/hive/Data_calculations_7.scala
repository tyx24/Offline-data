package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_7 {
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
        |COUNT(DISTINCT user_id) AS purchaseduser,
        |COUNT(DISTINCT CASE WHEN last_day - day = 1 THEN user_id END) AS repurchaseduser,
        |CONCAT(
        |        ROUND(COUNT(DISTINCT CASE WHEN last_day - day = 1 THEN user_id END) / COUNT(DISTINCT user_id) * 100, 1),
        |        '%'
        |    ) as repurchaserate
        | from
        |(select
        |*,
        |lead(`day`) over (partition by user_id order by `day`) as `last_day`
        |from
        |(select
        |user_id,
        |date_format(fact_order_info.create_time,"yyyyMMdd") as `day`
        |from
        |dwd.fact_order_info
        |)
        |)
        |""".stripMargin)

    dataFrame.write.mode("overwrite").jdbc(MysqlURL, "userrepurchasedrate", properties)

    sparkSession.read.jdbc(MysqlURL, "userrepurchasedrate", properties).show()

    sparkSession.stop()
  }
}
