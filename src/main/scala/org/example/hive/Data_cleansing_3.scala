package org.example.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Data_cleansing_3 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val warehouse = "hdfs://bigdata1:8020/user/hive/warehouse"

    val Conf = new SparkConf().setMaster("local[*]").setAppName("Data_cleansing_3")

    val sparkSession = SparkSession.builder().enableHiveSupport().config(Conf)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .getOrCreate()

    //获取ods数据
    val ods = sparkSession.sql("select * from ods.base_province where etl_date=20240401")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .drop("etl_date")

    //获取dwd数据
    val dwd = sparkSession.sql("select * from dwd.dim_province where etl_date=(select max(etl_date) from dwd.dim_province)")
      .drop("etl_date")

    ods.unionByName(dwd).createTempView("v")

    sparkSession.sql(
      """
        |from v
        |select *,
        | row_number() over (partition by v.id order by v.create_time desc) as create_time_num,
        | min(dwd_insert_time) over (partition by v.id) as min_dwd_insert_time
        |""".stripMargin)
      .withColumn("dwd_insert_time", when(col("create_time_num") === 1, col("min_dwd_insert_time"))
        .otherwise(col("dwd_insert_time")))
      .filter(col("create_time_num") === 1)
      .drop("create_time_num", "min_dwd_insert_time")
      .createTempView("result_v")

    sparkSession.sql("insert overwrite table dwd.dim_province partition(etl_date='20240401') select * from result_v")

    sparkSession.stop()
  }
}
