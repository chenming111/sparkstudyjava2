package cn.spark.study.demo;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkDataFromMysql {
	/**
	 * 扫描整个任务表，获取task_id和task_status字段，
	 * 如果task_status字段为0表示未进行wordcount处理，那么就
	 * 调用WordCountByTaskId进行处理，将结果存入数据库，并将task_status字段更新为1,表示已处理完；
	 * 如果task_status字段为1,表示已处理完，跳过。
	 */

	public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkDataFromMysql.class);
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://60.247.58.117:50012/spark_project";
	// 数据库的用户名与密码，需要根据自己的设置
	static final String USER = "root";
	static final String PASS = "Catarc@@123";
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL,USER,PASS);
		stmt = conn.createStatement();
		String sql;
		sql = "SELECT task_id, task_status FROM test_task";
		ResultSet rs = stmt.executeQuery(sql);
		JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().
				setAppName("SparkDataFromMysql").
				setMaster("local"));
		SQLContext sqlContext = new SQLContext(sparkContext);
		while(rs.next()){
			// 通过字段检索
			int task_id  = rs.getInt("task_id");
			int  task_status = rs.getInt("task_status");
			//task_status=0表示未处理，=1表示已处理
			if (task_status==1) continue;
			if (task_status==0) {
				WordCountByTaskId(task_id,sqlContext);
				PreparedStatement pstmt=conn.prepareStatement("update test_task set task_status=? where task_id =? ");
				pstmt.setInt(1,1);
				pstmt.setInt(2,task_id);
				pstmt.executeUpdate();
				pstmt.close();
				//stmt.executeUpdate("update test_task set task_status="+1 + " where task_id = "+task_id);
			}
		}
         conn.close();
		sparkContext.stop();
	}

	/**
	 * 根据taskid查询数据库获取任务参数task_param（要分析的文本）,进行wordcount分析后，结果存入数据库
	 * @param task_id
	 * @param sqlContext
	 */

	private static void WordCountByTaskId(final int task_id,SQLContext sqlContext){

		//接收任务号task_id

		String url = "jdbc:mysql://60.247.58.117:50012/spark_project";
		//查找的表名
		String table = "test_task";
		//增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
		Properties connectionProperties = new Properties();
		connectionProperties.put("user","root");
		connectionProperties.put("password","Catarc@@123");
		connectionProperties.put("driver","com.mysql.jdbc.Driver");

		// 读取表中所有数据
		DataFrame jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
		//显示数据
		// jdbcDF.show();
		//注册为临时表
		jdbcDF.registerTempTable("test_task");

		//sql查询结果为dataframe格式
		DataFrame taskparamDfByTaskId = sqlContext.sql("select task_param from test_task where task_id = "+task_id);
		/*
		 * taskparamDfByTaskId.show();
		 * taskparamDfByTaskId.printSchema();
		 */
		JavaRDD<Row> lines = taskparamDfByTaskId.toJavaRDD();
		JavaRDD<String> wordrdd = lines.flatMap(new FlatMapFunction<Row,String>() {

			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(Row t) throws Exception {

				return Arrays.asList(t.getString(0).split(","));
			}

		});

		JavaPairRDD<String, Integer> pairs = wordrdd.mapToPair(new PairFunction<String, String, Integer>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {

				return new Tuple2<String, Integer>(t, 1);
			}

		});

		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {

				return v1 + v2;
			}

		});
		//转换为（count,word),以便下面排序按照count使用sortbykey
		JavaPairRDD<Integer,String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {

				return new Tuple2<Integer, String>(t._2,t._1) ;
			}
		});

		//使用sortbykey按照count排序
		JavaPairRDD<Integer,String> sortedCountWords = countWords.sortByKey(false);
		//排序结果转换为（word,count)
		JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer,String>, String,Integer>(){

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {

				return new Tuple2<String, Integer>(t._2,t._1) ;
			}

		});

		//动态构造元数据
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("task_id", DataTypes.IntegerType, true),
				DataTypes.createStructField("word", DataTypes.StringType, true),
				DataTypes.createStructField("count", DataTypes.IntegerType, true)
		));

		//转化为Row类型
		JavaRDD<Row> rowsRdd = sortedWordCounts.map(new Function<Tuple2<String,Integer>,Row>(){

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Integer> v1) throws Exception {

				return RowFactory.create(Integer.valueOf(task_id),String.valueOf(v1._1),Integer.valueOf(v1._2));
			}

		});
		//构造结果DataFrame
		DataFrame sortedWordCountDataFrame = sqlContext.createDataFrame(rowsRdd, schema);
		//写入Mysql
		Properties properties = new Properties();
		properties.setProperty("user", "root");
		properties.setProperty("password", "Catarc@@123");
		sortedWordCountDataFrame.write().mode(SaveMode.Append).jdbc("jdbc:mysql://60.247.58.117:50012/spark_project", "test_task_result", properties);

		//打印结果
		/*
		 * sortedWordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
		 *
		 * private static final long serialVersionUID = 1L;
		 *
		 * @Override public void call(Tuple2<String, Integer> wordCount) throws
		 * Exception { System.out.println(wordCount._1 + " appeared " + wordCount._2 +
		 * " times."); }
		 *
		 * });
		 */

	}

}
