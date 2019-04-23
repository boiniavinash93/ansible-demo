package com.first.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class consumerGroups {

	public static void main(String[] args) 
	{
		Logger logger = LoggerFactory.getLogger(consumerGroups.class.getName());
		
		String bootstrapserver = "127.0.0.1:9092";
		String groupId = "my-first-app";
		String topic = "Demo";
		
		//Create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//subscribe consumer to our topics
		consumer.subscribe(Arrays.asList(topic));
		
		//subscribe consumer to Multiple topics
		//consumer.subscribe(Arrays.asList(topic, "test", "Demo_test"));
				
		//poll for new data
		while(true) 
		{
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> record : records ) {
				logger.info("key: " + record.key() + ", value: " + record.value());
				logger.info("partition: " + record.partition() + ", offset: " + record.offset());
			}
		}
		
		
	}
}
