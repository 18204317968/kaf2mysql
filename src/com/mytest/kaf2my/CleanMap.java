package com.mytest.kaf2my;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

public class CleanMap extends Thread{//定时清除CreateHDFS.hdfsMap.get("past")中的对象
	public void run(){
		for(;;){
			try {
				Thread.sleep(30*60*1000);
				synchronized (CreateHDFS.hdfsMap){
					ArrayList<String> list=new ArrayList<String>();
					Map<String,Object> map=CreateHDFS.hdfsMap.get("past");
					if (null != map&&map.size()>0) {
						for (Entry<String, Object> m : map.entrySet()) {
							if (null != m) {
								Date before = (Date)m.getValue();
								Date now =new Date();
								long l=now.getTime()-before.getTime();
								if(l>KafkaProperties.HISTRYTIME*60*1000){
									list.add(m.getKey());
								}
							}
						}
						if(null!=list&&list.size()>0){
							for(String s:list){
								map.remove(s);
							}
						}
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
