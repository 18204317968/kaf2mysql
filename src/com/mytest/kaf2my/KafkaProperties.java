package com.mytest.kaf2my;

import java.util.ResourceBundle;

public interface KafkaProperties {  
	
	public final static ResourceBundle BUNDLE = ResourceBundle.getBundle("resource"); 
	
    final static String zkConnect = BUNDLE.getString("zkConnect");  
	final static String dbUrl =BUNDLE.getString("dbUrl");
    
	final static String groupId1= BUNDLE.getString("groupId1"); 
    
    final static String zkConnect_work = BUNDLE.getString("zkConnect_work");
    final static String topic_new = BUNDLE.getString("topic_new");  
    final static String topic_soc = BUNDLE.getString("topic_soc"); 
    final static String kafkaServerURL_work = BUNDLE.getString("kafkaServerURL_work"); 
    
    
    final static String kafkaServerURL = BUNDLE.getString("kafkaServerURL");  
    final static int kafkaServerPort =  Integer.parseInt(BUNDLE.getString("kafkaServerPort"));
    
    final static String BETWEEN = BUNDLE.getString("BETWEEN");
    final static String BEGIN = BUNDLE.getString("BEGIN");
    final static String NEWDIR = BUNDLE.getString("NEWDIR");
    final static String SOCDIR = BUNDLE.getString("SOCDIR");
    //final static int kafkaProducerBufferSize = 64 * 1024;  
    //final static int connectionTimeOut = 20000;  
    //final static int reconnectInterval = 10000;  
    //final static String clientId = "SimpleConsumerDemoClient";  
}  