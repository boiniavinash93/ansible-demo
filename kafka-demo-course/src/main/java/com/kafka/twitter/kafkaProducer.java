package com.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.first.demo.producer_callback;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class kafkaProducer {
	
	
	Logger logger = LoggerFactory.getLogger(kafkaProducer.class.getName());
	
	String consumerKey = "YLcK3EjdPceLpstBO3PcC1tu5";
	String consumerSecret = "xDSGDxzxnfwPZYI6CQD2gVkDoo3y8Tl4ardQPdxikPQSoXgCev";
	String token = "1021716070258733056-CBStv6lADGJHfeoDRbRtLfOo1fHlda";
	String secret = "T5Ru0XwRTM1W6RYxm3CegTDf770OrQ5HcyGSJD0hKO9PT";
	
	//Constructor for producer class
	public kafkaProducer() {}
	
	public static void main(String[] args) 
	{
		new kafkaProducer().run();
	}

	public void run() {
		
		logger.info("Setup");
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
				
		//Create a twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		
		//Create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		
		//loop to send tweet to kafka
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
		  String msg = null;
		  try {
			   msg = msgQueue.poll(5,TimeUnit.SECONDS); 
			}catch(InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
		  if (msg != null) 
		  {
			  logger.info(msg);
			  producer.send(new ProducerRecord<String, String>("twitter_tweets1", null, msg), new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					if(exception != null) 
					{
						logger.error("Something went wrong", exception);
					}
					
				}
			});
		  }
		}
		logger.info("End of application");
	}
	

	public Client createTwitterClient(BlockingQueue<String> msgQueue) 
	{
				
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("ipl");
		hosebirdEndpoint.trackTerms(terms);
		
		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
				                     

				Client hosebirdClient = builder.build();
				return hosebirdClient;
	
	}
	
	public KafkaProducer<String, String> createKafkaProducer()
	{

		String bootStrapServer = "127.0.0.1:9092";
		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}
	
	
}
