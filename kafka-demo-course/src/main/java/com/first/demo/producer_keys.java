package com.first.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.google.common.util.concurrent.ExecutionError;


public class producer_keys {

	public static void main(String[] args) throws ExecutionException, InterruptedException
	{
		Logger logger = LoggerFactory.getLogger(producer_keys.class);
		
		String bootStrapServer = "127.0.0.1:9092";
	//create producer properties
	Properties properties = new Properties();
	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
	//create producer
	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
	
	for (int i=0; i<10; i++) {
	
		String topic = "test";
		String value = "Hello";
		String key = "id_" + Integer.toString(i);
		
	//producer record
	ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,value);
	
	logger.info("key: " + key);
	//send data
	producer.send(record, new Callback() {
		
		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub
			// executes every time a record is successfully sent or an exception is thrown
			if(exception == null) 
			{
				// the record was successfully sent
				logger.info("Received new metadata. \n" +
						"Topic: " + metadata.topic() + "\n" +
						"partiton: " + metadata.partition() + "\n" +
						"Offset: " + metadata.offset() + "\n" +
						"Timestamp: " + metadata.timestamp());
				
			}else {
				
				logger.error("Error while producing", exception);
			}
			
		
		}
	}).get(); //this is to block the .send() to make it synchronous -- but don't do it in production
	}
	//flush data
	producer.flush();
	producer.close();
	
	}
}
