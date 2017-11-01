package com.mytest.kaf2my;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KafMsgTest {  
	  
    public static void main(String[] args) throws Exception{  
    	 
//        	 System.out.println("kafka.............start");
        	 KafMsgTest kfk=new KafMsgTest();
        	 kfk.doShutdownHook();
//    		 KafkaConsumer consumerThread1 = new KafkaConsumer("news"); 
//    		 KafkaConsumer consumerThread2 = new KafkaConsumer("soc"); 
//             consumerThread1.start();
//             consumerThread2.start();
        	 Configuration conf=new Configuration();
        	 conf.set("fs.default.name", "hdfs://192.168.2.51:9000");
        	 conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        	 conf.setBoolean("dfs.support.append", true);
        	 FileSystem fs=FileSystem.get(conf);
        	 FSDataOutputStream creae=fs.create(new Path("hdfs://192.168.2.51:9000/myspace/hadoop/hdfs_file/test.txt"));
        	 System.out.println("create!!");
        	 creae.write("Hello world".getBytes("UTF-8"));
        	 System.out.println("write!!");
        	 creae.flush();
        	 System.out.println("flush!!");
        	 creae.close();
        	 System.out.println("close!!");
        	 fs.close();
        	 FileSystem dfs=FileSystem.get(conf);
        	 FSDataOutputStream append=dfs.append(new Path("hdfs://192.168.2.51:9000/myspace/hadoop/hdfs_file/test.txt"));
        	 System.out.println("append!!");
        	 for(int i=0;i<100;i++){
        		 append.write("    !".getBytes("UTF-8"));
        		 System.out.println("appendwriteing!!");
        		 append.flush();
        		 System.out.println("appendflushing!!");
        		 System.out.println(i);
        	 }
        	 append.close();
        	 System.out.println("appendcloseing!!");
        	 dfs.close();
        	 System.out.println("totalcloseing!!");
    	 }
          
         private void doShutdownHook(){
        	 Runtime.getRuntime().addShutdownHook(new Thread(){
        		 public void run(){
        			 try{
        				 if(null!=CreateHDFS.newsOutputStream){
        					 CreateHDFS.newsOutputStream.close();
        					 CreateHDFS.newsHdfs.close();
        				 }
        				 if(null!=CreateHDFS.socOutputStream){
        					 CreateHDFS.socOutputStream.close();
        					 CreateHDFS.socHdfs.close();
        				 }
        				 System.out.println("end");
        			 }catch(Exception e){
        				 e.printStackTrace();
        			 }
        		 }
        	 });
         }
    
}  
