package KafkaSparkStreaming;

import Model.IOTModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;

public class KafkaProducerIOT {

    public static final String device3 = "11c1310e-c0c2-461b-a4eb-f6bf8da2d23d";
    public static final String device2 = "873ac084-ac86-420e-a23f-ba1c3eb81a4c";
    public static final String device1 = "902550ae-916e-4f0a-ba54-97c8e24407b4";
    public static IOTModel iotD1 ;
    public static IOTModel iotD2 ;
    public static IOTModel iotD3 ;
    public static StringWriter stringIOTD1;
    public static StringWriter stringIOTD2;
    public static StringWriter stringIOTD3;
    public static ProducerRecord<String, String> producerRecord;

    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        //JSON Generation
        //create ObjectMapper instance
        ObjectMapper objectMapper;

        try {
            while (true) {
                try {
                    //convert Object to json string
                    objectMapper = new ObjectMapper();
                    //configure Object mapper for pretty print
                    objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

                    //writing to console, can write to any output stream such as file
                    stringIOTD1 = new StringWriter();
                    stringIOTD2 = new StringWriter();
                    stringIOTD3 = new StringWriter();

                    iotD1 = IOTSimulator.createIOTSimulation(device1);
                    iotD2 = IOTSimulator.createIOTSimulation(device2);
                    iotD3 = IOTSimulator.createIOTSimulation(device3);
					
                    objectMapper.writeValue(stringIOTD1, iotD1);
                    objectMapper.writeValue(stringIOTD2, iotD2);
                    objectMapper.writeValue(stringIOTD3, iotD3);
					
                } catch (IOException e) {
                    e.printStackTrace();
                }

                producerRecord = new ProducerRecord<String, String>("testTopic1", "data", stringIOTD1.toString()+"$$"
                                        +stringIOTD2.toString()+"$$"+stringIOTD3.toString());

                producer.send(producerRecord);
                Thread.sleep(1000);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            producer.close();
        }
    }
}
