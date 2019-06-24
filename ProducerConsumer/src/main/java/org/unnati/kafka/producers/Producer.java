package org.unnati.kafka.producers;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unnati.kafka.utils.Constants;

public class Producer {

    private Logger logger= LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        (new Producer()).createProducer();
    }
    
    private void createProducer(){
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(this.initConfig());
        for(int i=1;i<=100;i++) {
            producer.send(this.getProducerRecord("Message-" + i), new KafkaCallback());
            producer.flush();
        }   
        producer.close();
    }
    
    
    private static AtomicInteger counter=new AtomicInteger(0);
    
    private ProducerRecord<String,String> getProducerRecord(String value){
        String key=Constants.KAFKA_TOPIC+"-key-"+counter.getAndIncrement();
        //new ProducerRecord<String, String>(topic ,value) --> This will create a record will null key. If ket is not null
        // next message with same key will go to same partition 
        return new ProducerRecord<String, String>(Constants.KAFKA_TOPIC,key,value);
    }
    
    // Create Properties
    private Properties initConfig(){
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_CLUSTER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
    
    
    //By Default this call is async
    class KafkaCallback implements Callback{

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e==null){
                logger.info("Message has been send successfully \n"+
                        "Topic:"+recordMetadata.topic()+"\n"+
                        "Partition:"+recordMetadata.partition()+"\n"+ 
                        "Offset:"+recordMetadata.offset()+"\n"+
                        "TimeStamp:"+recordMetadata.timestamp());
            }else{
                e.printStackTrace();
            }
        }
    }
}
