package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_5 {
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
        |SELECT
        |*,
        |case
        | when (provinceavgconsumption>regionavgconsumption) then "高"
        | when (provinceavgconsumption<regionavgconsumption) then "低"
        | else "相同" end as `comparison`
        |FROM
        | (SELECT
        |   province_id provinceid,
        |   province_name provincename,
        |   total_amount/total_count provinceavgconsumption,
        |   region_id regionid,
        |   region_name regionname,
        |   sum(total_amount) over (partition by `year`,`month`,region_id)/
        |   sum(total_count) over (partition by `year`,`month`,region_id) as regionavgconsumption
        | FROM dws.province_consumption_day_aggr
        | where year=2020 and month=04
        | )
        |""".stripMargin)

    dataFrame.write.mode("overwrite").jdbc(MysqlURL, "provinceavgcmpregion", properties)

    sparkSession.read.jdbc(MysqlURL, "provinceavgcmpregion", properties).show()

    sparkSession.stop()
  }
}
