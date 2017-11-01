package com.mytest.kaf2my;

import java.io.IOException;   
import org.apache.hadoop.conf.Configuration;   
import org.apache.hadoop.hbase.HBaseConfiguration;   
import org.apache.hadoop.hbase.HColumnDescriptor;   
import org.apache.hadoop.hbase.HTableDescriptor;   
import org.apache.hadoop.hbase.client.HBaseAdmin;  
public class CreatTableTest {  
	
    public static void main(String[] args) throws IOException  {   
          
        Configuration conf = HBaseConfiguration.create();   
        conf.set("hbase.zookeeper.quorum",  "mybook1,mybook2,mybook3");  //  Zookeeper�ĵ�ַ  
        conf.set("hbase.zookeeper.property.clientPort", "2181");   
        String tableName = "emp";  
        String[] family = { "basicinfo","deptinfo"};   
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);   
        
        HTableDescriptor hbaseTableDesc = new HTableDescriptor(tableName);   
        for(int i = 0; i < family.length; i++) {   
        
        hbaseTableDesc.addFamily(new HColumnDescriptor(family[i]));   
  
        }   
         
        if(hbaseAdmin.tableExists(tableName)) {   
        System.out.println("TableExists!");   
        System.exit(0);   
        } else{   
        hbaseAdmin.createTable(hbaseTableDesc);   
        System.out.println("Create table Success!");   
        }   
    }   
}  