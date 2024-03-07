package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_2 {
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
        |*,
        |CASE WHEN (provinceavgconsumption > allprovinceavgconsumption) THEN '高'
        |     WHEN (provinceavgconsumption < allprovinceavgconsumption) THEN '低'
        |     ELSE '相同' END AS comparison
        |from
        |(select
        |  province.id provinceid,
        |  province.name providerincename,
        | avg(order.final_total_amount) provinceavgconsumption,
        | AVG(AVG(order.final_total_amount)) OVER () AS allprovinceavgconsumption
        | from dwd.fact_order_info order
        | join dwd.dim_province province
        | on (order.province_id = province.id
        |      and province.etl_date='20240401')
        | WHERE year(order.create_time)=2020
        | and month(order.create_time)=04
        |  GROUP BY province.id, province.name
        |  )
        |""".stripMargin)

    dataFrame.write.mode("overwrite").jdbc(MysqlURL, "provinceavgcmp", properties)

    sparkSession.read.jdbc(MysqlURL, "provinceavgcmp", properties).show()

    sparkSession.stop()
  }
}
