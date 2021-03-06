package cn.spark.study.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * k-means 示例
  * 源数据为：
  * 0.0 0.0 0.0
  * 0.1 0.1 0.1
  * 0.2 0.2 0.2
  * 0.3 0.3 0.3
  * 0.4 0.4 0.4
  * 0.5 0.5 0.5
  * 9.0 9.0 9.0
  * 9.1 9.1 9.1
  * 9.2 9.2 9.2
  * 9.3 9.3 9.3
  * 9.4 9.4 9.4
  * 9.5 9.5 9.5
  */

object KmeansTest {
  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 设置运行环境
    val conf = new SparkConf().setAppName("KmeansTest").setMaster("local")
    val sc = new SparkContext(conf)

    //装载数据集
    val data = sc.textFile("C:\\Users\\chenming\\Desktop\\kmeans_data.txt", 1)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    //将数据集聚类，2个类，20次迭代，形成数据模型
    val numClusters = 2
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)

    //数据模型的中心点
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println(" " + c.toString)
    }

    //使用误差平方之和来评估数据模型
    val cost = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + cost)

    //使用模型测试单点数据
    println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
    println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
    println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))

    //交叉评估1，只返回结果
    val testdata = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val result1 = model.predict(testdata)
    result1.saveAsTextFile("C:\\Users\\chenming\\Desktop\\result1")

    //交叉评估2，返回数据集和结果
    val result2 = data.map {
      line =>
        val linevectore = Vectors.dense(line.split(' ').map(_.toDouble))
        val prediction = model.predict(linevectore)
        line + " " + prediction
    }.saveAsTextFile("C:\\Users\\chenming\\Desktop\\result2")

    sc.stop()


  }



}
