package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Data_calculations_3 {
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
        |userid,
        |username,
        |concat_ws("_",day,lead_day) as `day`,
        |(sum_day_totalconsumption+lead_totalconsumption) as totalconsumption,
        |(count_day_totalorder+lead_totalorder) as totalorder
        |from
        |(
        |select
        |userid,
        |username,
        |`day`,
        |sum_day_totalconsumption,
        |count_day_totalorder,
        |lead(`day`) over (partition by userid,username order by day) as lead_day,
        |lead(sum_day_totalconsumption) over (partition by userid,username order by day) as lead_totalconsumption,
        |lead(count_day_totalorder) over (partition by userid,username order by day) as lead_totalorder
        |from(
        |select
        | dim_user_info.id as userid,
        | name as username,
        | date_format(fact_order_info.create_time,"yyyyMMdd") as `day`,
        | sum(`final_total_amount`) as sum_day_totalconsumption,
        | count(*) as count_day_totalorder
        |from
        | dwd.fact_order_info
        | join
        | dwd.dim_user_info
        | on (dim_user_info.id = user_id
        |     and dim_user_info.etl_date = '20240401')
        | group by dim_user_info.id, name,
        |          date_format(fact_order_info.create_time,"yyyyMMdd")
        |  )
        |   )
        |   where lead_day-day = 1
        |         and sum_day_totalconsumption < lead_totalconsumption
        |""".stripMargin)

    dataFrame.write.mode("overwrite").jdbc(MysqlURL, "usercontinueorder", properties)

    sparkSession.read.jdbc(MysqlURL, "usercontinueorder", properties).show()

    sparkSession.stop()
  }
}