package com.mytest.kaf2my;

import java.util.ResourceBundle;

public interface KafkaProperties {

	public final static ResourceBundle BUNDLE = ResourceBundle
			.getBundle("resource");

	final static String zkConnect = BUNDLE.getString("zkConnect");
	final static String dbUrl = BUNDLE.getString("dbUrl");
	final static String groupId1 = BUNDLE.getString("groupId1");
	final static String zkConnect_work = BUNDLE.getString("zkConnect_work");
	final static String topic_new = BUNDLE.getString("topic_new");
	final static String topic_soc = BUNDLE.getString("topic_soc");
	final static String kafkaServerURL_work = BUNDLE.getString("kafkaServerURL_work");
	final static String kafkaServerURL = BUNDLE.getString("kafkaServerURL");
	final static int kafkaServerPort = Integer.parseInt(BUNDLE.getString("kafkaServerPort"));
	final static String BETWEEN = BUNDLE.getString("BETWEEN");//数据列分隔符
	final static String BEGIN = BUNDLE.getString("BEGIN");//数据行分隔符
	final static String SOURCEDIR = BUNDLE.getString("SOURCEDIR");//hdfs文件生成目录
	final static String FSDEFAULTNAME = BUNDLE.getString("FSDEFAULTNAME");//fileSystem指定配置路径
	final static String CLUSTERNAME = BUNDLE.getString("CLUSTERNAME");//集群名称
	final static String NN1 = BUNDLE.getString("NN1");//NN1指定配置路径
	final static String NN2 = BUNDLE.getString("NN2");//NN2指定配置路径
	final static String LOGSDIR = BUNDLE.getString("LOGSDIR");//hdfs中日志存放路径
	final static String INTERFACE = BUNDLE.getString("INTERFACE");//新闻正文接受接口
	final static int TIMEOUT = Integer.parseInt(BUNDLE.getString("TIMEOUT"));//接受消息超时时间（单位：s）
	final static int HISTRYTIME = Integer.parseInt(BUNDLE.getString("HISTRYTIME"));//防止重复写入时间（这段时间无法重复写入已完成的文件）（单位：ms）
	final static int SIZE = Integer.parseInt(BUNDLE.getString("SIZE"));//分割文件的大小（单位：MB）
	// final static int kafkaProducerBufferSize = 64 * 1024;
	// final static int connectionTimeOut = 20000;
	// final static int reconnectInterval = 10000;
	// final static String clientId = "SimpleConsumerDemoClient";
	// final static String NEWDIR = BUNDLE.getString("NEWDIR");
	// final static String SOCDIR = BUNDLE.getString("SOCDIR");
}