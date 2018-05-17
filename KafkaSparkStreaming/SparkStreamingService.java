package KafkaSparkStreaming;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

import Model.IOTModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

public class SparkStreamingService {
    public static ObjectMapper objectMapper;
    public static IOTModel iotData;
    public static String jsonMessages;
    public static String jsonMessage;
    public static Date timestamp;
    public static Put p;

    public static void main(String[] args) throws IOException {
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "SparkStream1");
        kafkaParams.put("zookeeper.connect", "localhost:2181");

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Set<String> topics = new HashSet<String>(Arrays.asList("testTopic".split(",")));

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        //HBase Configuration
        Configuration conf = HBaseConfiguration.create();
		
        // Instantiating HTable class
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testhbase"));


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("logger1");
        logger.log(Level.INFO,"Logger Started");

        messages.foreachRDD(
                (Function<JavaPairRDD<String, String>, Void>) rdd -> {
					
                    logger.log(Level.INFO,"ForEach function..");
					
                    System.out.println("Rdd Count"+rdd.count());
					
                    List<Tuple2<String,String>> list = rdd.collect();
					
                    for(Tuple2<String,String> tuple : list) {
						
                        logger.log(Level.INFO, "Reading Tuple");
						
                        jsonMessages = tuple._2.toString();
						
                        for (String jsonMessage : jsonMessages.split("\\$\\$")) {
							
							
                            objectMapper = new ObjectMapper();
                            iotData = objectMapper.readValue(jsonMessage, IOTModel.class);

                            timestamp = new Date(Long.parseLong(iotData.getData().getTime()));
							
							// Instantiating Put class
                            p = new Put(Bytes.toBytes(iotData.getData().getTime()));

                            p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("deviceId"), Bytes.toBytes(iotData.getData().getDeviceId()));
                            p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("temperature"), Bytes.toBytes(iotData.getData().getTemperature()));
                            p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("locLatitude"), Bytes.toBytes(iotData.getData().getLocation().getLatitude()));
                            p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("locLongitude"), Bytes.toBytes(iotData.getData().getLocation().getLongitude()));
                            p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("time"), Bytes.toBytes(sdf.format(timestamp)));

                            table.put(p);

                        System.out.println("data inserted");
                        logger.log(Level.INFO,"Ending Tuple");
                        }
                    }
                    return null;
                }
        );

        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
