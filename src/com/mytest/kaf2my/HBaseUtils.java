package com.mytest.kaf2my;

import java.io.IOException;  
import java.util.Random;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.util.Bytes;  
public class HBaseUtils {  
	
    public  void put(String string) throws IOException {   
    
    Configuration conf = HBaseConfiguration.create();   
    conf.set("hbase.zookeeper.quorum",  "mybook1,mybook2,mybook3");  //  Zookeeper 
    conf.set("hbase.zookeeper.property.clientPort", "2181");   
    Random random = new Random();  
    long a = random.nextInt(1000000000);             
    String tableName = "emp";   
    String rowkey = "rowkey"+a ;  
    String columnFamily = "basicinfo";   
    String column = "empname";   
    //String value = string;   
    HTable table=new HTable(conf, tableName);   
    Put put=new Put(Bytes.toBytes(rowkey));   
    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(string));   
    table.put(put);//  
    table.close();// 
    }  
}  