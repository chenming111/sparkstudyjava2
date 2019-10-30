package cn.spark.study.demo

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object ReadDataFromMysql {
  def main(args: Array[String]): Unit = {
    // 设置程序入口参数
    val conf = new SparkConf().setAppName("ReadDataFromMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建数据库链接，加载数据，默认转为df
    val jdbcDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://60.247.58.117:50012/spark_project",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "test_task",
        "user" -> "root",
        "password" -> "Catarc@@123")).load()

    // df 注册为表，方便sql操作
    jdbcDF.registerTempTable("test_task")
    sqlContext.sql("select task_param from test_task where task_id = 1").show()
    val taskparamdf = sqlContext.sql("select task_param from test_task where task_id = 1")
    taskparamdf.groupBy("task_param").count().show()

    //val wordrdd = taskparamdf.rdd
   sc.stop()
  }
}