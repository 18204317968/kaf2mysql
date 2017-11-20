package com.mytest.kaf2my;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import com.teamsun.kafka.m001.KafkaProperties;  

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread {

	private final ConsumerConnector consumer;
	private String topic;

	// public static InsertDB m_idb = new InsertDB();
	public static CreateHDFS m_chdfs = new CreateHDFS();
	// public static Connection m_conn =null;
	// public static String m_msgType="";
	public static boolean m_workFlag;

	public KafkaConsumer(String mt) {

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());

		if (mt.equals("news")) {
			this.topic = KafkaProperties.topic_new;
		} else if (mt.equals("soc")) {
			this.topic = KafkaProperties.topic_soc;
		}

		// m_msgType = mt;

		/*
		 * InputStream inputStream =
		 * this.getClass().getClassLoader().getResourceAsStream
		 * ("cfg.properties"); Properties p = new Properties(); try {
		 * p.load(inputStream); } catch (Exception e1) { e1.printStackTrace(); }
		 * /
		 * /System.out.println(“ip:”+p.getProperty(“ip”)+“,port:”+p.getProperty(
		 * “port”));
		 */

	}

	private static ConsumerConfig createConsumerConfig() {

		Properties props = new Properties();

		// props.put("zookeeper.connect", KafkaProperties.zkConnect); //test
		// zookeepr addr
		props.put("zookeeper.connect", KafkaProperties.zkConnect_work);

		props.put("group.id", KafkaProperties.groupId1);
		props.put("zookeeper.session.timeout.ms", "5000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.connection.timeout.ms", "10000");
		props.put("rebalance.backoff.ms", "2000");
		props.put("rebalance.max.retries", "10");

		// try{//init db conn
		// m_conn = m_idb.getConn(KafkaProperties.dbUrl);
		// }catch(Exception ex){
		// System.out.println(ex.toString());
		// }

		return new ConsumerConfig(props);
	}

	// private static void doKafMsg (Connection con,String str) {
	// m_idb.doMsg(con,m_msgType,str);
	// }

	private static void doHDFS(String msg, String str) throws Exception {
		m_chdfs.doHDFS(msg, new Date(), str);
	}

	@Override
	public void run() {
		System.out.println(topic);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		CleanMap clean=new CleanMap();//定时清除hdfsmap中存入的以写文件
		clean.start();
		while (it.hasNext()) {

			String msg = new String(it.next().message());
			try {
				if (topic.equals(KafkaProperties.topic_new)) {
					doHDFS(msg, "news");//写入新闻数据
				} else {
					doHDFS(msg, "soc");//写入社交数据
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
