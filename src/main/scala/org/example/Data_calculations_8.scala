package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Data_calculations_8 {
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

    val dataFrame = sparkSession.sql(
      """
        |SELECT
        |  name as province_name,
        |  count(*) AS Amount
        |FROM
        |  dwd.fact_order_info
        |  join dwd.dim_province
        |  on (province_id = dim_province.id
        |     and dim_province.etl_date='20240401')
        |GROUP BY
        |  name
        |ORDER BY
        |  Amount DESC
      """.stripMargin)

    val array: Array[String] = dataFrame.collect().map(x => {
      val province: String = x(0).toString
      province
    })

    // 设置dataframe表头类型
    val fields: Array[StructField] = array.map(fieldName => {
      StructField(fieldName, StringType, nullable = true)
    })
    val schema: StructType = StructType(fields)
    // 将多行数据转换一行
    /**
     * collect_list:不去重
     * collect_set:去重
     */
    val result: RDD[Row] = dataFrame
      .select("amount")
      .withColumn("amount", collect_list(col("amount")))
      .rdd
      .map(x => {
        // 截取数组内的数据
        val left: Int = x(0).toString.indexOf("(")
        val right: Int = x(0).toString.indexOf(")")
        x(0).toString.substring(left + 1, right).replace(" ", "")
      })
      .map(_.split(",").sorted.reverse) //todo 倒序排序
      .map(x => Row(x: _*)) //todo 创建dataframe

    sparkSession.createDataFrame(result, schema).show(false)

    sparkSession.stop()
  }
}