package org.example.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object Data_calculations_4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.ERROR)

    //warehouse
    val warehouse = "hdfs://bigdata1:9000/user/hive/warehouse"

    val Conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Data_calculations_4")

    val sparkSession = SparkSession.builder().config(Conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .enableHiveSupport()
      .getOrCreate()

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")

    val URL1 = "jdbc:mysql://bigdata1:3306/shtd_store"
    val URL2 = "jdbc:mysql://bigdata1:3306/shtd_result"

    sparkSession.read.jdbc(URL1, "order_info", properties).createTempView("v")

    val dataFrame = sparkSession.sql(
      """
        |SELECT
        |id as order_id,
        |ABS(SUM(final_total_amount) over (order by id) - 20330606) AS diff_value
        |from v
        |ORDER BY diff_value desc
        |limit 10
        |""".stripMargin)

    dataFrame.write.mode(SaveMode.Append).jdbc(URL2, "order_final_money_amount_diff", properties)

    sparkSession.read.jdbc(URL2, "order_final_money_amount_diff", properties).show()

    sparkSession.stop()
  }
}
