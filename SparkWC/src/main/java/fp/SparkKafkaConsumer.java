package fp;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		System.out.println("Spark Streaming started now .....");                                                                                                    

		SparkConf conf = new SparkConf().setAppName("Weather").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
		ssc.checkpoint("hdfs://quickstart.cloudera:8020/user/hive/warehouse/weatherCheck");

		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		Set<String> topics = Collections.singleton("twitter-kafka");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		   
		  
		  directKafkaStream.foreachRDD(rdd -> {
			  
			  if(rdd.count() > 0) {
				  SparkSession ss = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

				  JavaRDD<String> rowRDD = rdd.map((row) -> {
					  String date = row._2.split(",")[0];
					  double temp = Double.parseDouble(row._2.split(",")[3]);
					  StringBuilder sb = new StringBuilder(); 
					  sb.append(date).append(":").append(temp);
			          return sb.toString();
				  });
				  
				  Dataset wordsDataFrame = ss.createDataFrame(rowRDD, String.class);
				  wordsDataFrame.createOrReplaceTempView("weatherTemp");
				  
				  ss.sql("create table weather as select * from weatherTemp");
				  
			  }
		  });
		 
		ssc.start();
		ssc.awaitTermination();
	}
	

}

