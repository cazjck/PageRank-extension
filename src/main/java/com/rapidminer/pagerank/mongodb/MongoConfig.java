package com.rapidminer.pagerank.mongodb;


import com.mongodb.MongoClient;
/**
 * 
 * MongoConfig using for test in localhost
 *
 */
public class MongoConfig {
	private static MongoClient mongoClient = null;
	public static String HOST = "localhost";
	public static int PORT = 27017;
/**
 * Instance Hadoop Cluster
 * @return
 */
	public static MongoClient getInstance() {
		if (mongoClient == null) {
			mongoClient=new MongoClient(HOST, PORT);

		}
		return mongoClient;
	}
}
