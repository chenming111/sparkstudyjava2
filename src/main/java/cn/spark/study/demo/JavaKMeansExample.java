package cn.spark.study.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * spark ml K-means  java示例
 */
public class JavaKMeansExample {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(JavaKMeansExample.class);
        // 设置日志的等级 并关闭jetty容器的日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().
                setMaster("local").
                setAppName("JavaKMeansExample");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "C:\\Users\\Administrator\\Desktop\\kmeans_data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        int numClusters = 2;
        int numIterations = 20;
        int runs = 10;
        /**
         * KMeans.train(RDD<Vector> data, int k, int maxIterations, int runs, String initializationMode, long seed) data 进行聚类的数据 k
         * 初始的中心点个数 maxIterations 迭代次数
         * runs 运行次数
         * initializationMode 初始中心点的选择方式, 目前支持随机选 "random" or "k-means||"。默认是 K-means||
         * seed 集群初始化时的随机种子。
         */
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations, runs);
        // 输出聚类的中心
        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }

        // 本次聚类操作的收敛性，此值越低越好
        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        // 预测并输出输出每组数据对应的中心
        parsedData.foreach(f -> {
            System.out.print(f + "\n");
            System.out.println(clusters.predict(f));
        });
        // 预测数据属于哪个中心点
        int centerIndex = clusters.predict(Vectors.dense(new double[] {3.6, 4.7, 7.1}));//中心点的索引
        System.out.println("预测数据 (3.6, 4.7, 7.1)属于中心[" + centerIndex + "]:" + clusters.clusterCenters()[centerIndex]);

        centerIndex = clusters.predict(Vectors.dense(new double[] {1.1, 0.7, 0.3}));
        System.out.println("预测数据 (1.1,0.7, 0.3)属于中心[" + centerIndex + "]:" + clusters.clusterCenters()[centerIndex]);
        jsc.stop();

    }

}
