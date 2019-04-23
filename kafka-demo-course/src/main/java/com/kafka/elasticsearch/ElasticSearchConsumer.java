package com.kafka.elasticsearch;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {
	
	public static RestHighLevelClient createClient()
	{
		// https://nq0bm3vtoa:ufv1p66x3e@kafka-demo-elasticse-7770965000.us-east-1.bonsaisearch.net
		
		String hostname = "kafka-demo-elasticse-7770965000.us-east-1.bonsaisearch.net";
		String username = "nq0bm3vtoa";
		String password = "ufv1p66x3e";
		
		// dont do if you run a local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, 
				new UsernamePasswordCredentials(username, password));
		
		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	public static void main(String[] args) throws IOException
	{
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		
		RestHighLevelClient client = createClient();
		
		String jsonString = "{ \"foo\": \"bar\" }";
		
		IndexRequest indexRequest = new IndexRequest(
				"twitter",
				"tweets"
				).source(jsonString, XContentType.JSON);
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		String id = indexResponse.getId();
		logger.info(id);
		
		//close the client gracefully
		client.close();
	}

}
