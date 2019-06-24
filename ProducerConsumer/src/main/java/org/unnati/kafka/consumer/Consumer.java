package org.unnati.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unnati.kafka.utils.Constants;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    Logger logger= LoggerFactory.getLogger(Consumer.class);
    public static void main(String[] args) {
        (new Consumer()).run();
       
    }

    public void run()   {
        CountDownLatch latch=new CountDownLatch(1);
        ConsumerRunnable consumerRunnable=new ConsumerRunnable(latch);
        (new Thread(consumerRunnable)).start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Application Shutdown has been requested");
            try {
                consumerRunnable.shutdown();
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has been initiated ");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Application exists ");
    }
  
    
    
    class ConsumerRunnable implements Runnable{
        
        Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String,String> kafkaConsumer;
        private CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch latch){
            // Create Consumer
            kafkaConsumer=new KafkaConsumer<String, String>(this.initConfig());
            // Subscribe Consumer
            kafkaConsumer.subscribe(Collections.singleton(Constants.KAFKA_TOPIC));
            this.latch=latch;
        }

        public void run() {
            
            try{
                while(true) {
                    ConsumerRecords<String,String> records=kafkaConsumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String,String> consumerRecord:records){
                        logger.info("Key :"+consumerRecord.key()+"\n"+
                                "Value :"+consumerRecord.value()+"\n"+
                                "partition :"+consumerRecord.partition()+"\n"+
                                "topic :"+consumerRecord.topic());
                    }
                }    
            } catch (WakeupException e){
                logger.info("Consumer has been notified to shut down");
            }finally {
                this.kafkaConsumer.close();
                logger.info("Consumer has been shut down");
                latch.countDown();
            }
        }
        
        public void shutdown(){
            kafkaConsumer.wakeup();
        }

        // Create Properties
        private Properties initConfig(){
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_CLUSTER);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_NAME1);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.KAFKA_CONSUMER_OFFSET_EARLIEST);
            return properties;
        }
    }
}
