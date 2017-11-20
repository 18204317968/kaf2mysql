package com.mytest.kaf2my;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TimeOutJudge extends Thread {
	private String fileName;

	public TimeOutJudge(String fileName) {
		this.fileName = fileName;
	}
	/*写日志*/
	public void writeLog(String str,Date date,String fileName,String specialId,String mt) {
		try {
			SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
			PrintStream log = new PrintStream("./log"+format.format(date)+".txt");//记录到项目本地的日志文件名
			PrintStream out = System.out;
			System.setOut(log);
			System.out.println(str);
			System.setOut(out);
			CreateHDFS.writeLog(fileName, specialId, mt, 1);//将失败信息同步到hdfs中
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/*删除文件*/
	public void deleteFile(Map<String, Object> map){
		try{
			FileSystem fileSystem=(FileSystem)map.get("fileSystem");
			FSDataOutputStream outPutStream=(FSDataOutputStream)map.get("outputStream");
			if(null!=outPutStream){//先关闭输出流
				outPutStream.close();
				System.out.println("streamClose...");
			}
			if(null!=fileSystem){//关闭文件系统
				fileSystem.close();
				System.out.println("fileSystemClose...");
			}
			Map<String, Object> pastMap=(Map<String, Object>)CreateHDFS.hdfsMap.get("past");
			pastMap.put(fileName, new Date());//记录文件已经写过
			String name=(String)map.get("names");//取到所有文件名
			String[] names=name.split("&&");//拆开装入数组中
//			Configuration conf=new Configuration();
//			conf.set("fs.default.name", KafkaProperties.FSDEFAULTNAME);
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			FileSystem delFileSystem=FileSystem.get(conf);
			if(null!=names&&names.length>0){
				for(String n:names){
					CreateHDFS.fileSystem.delete(new Path(n),true);//执行删除
				}
			}
			System.out.println("delete...");
			CreateHDFS.hdfsMap.remove(fileName);//移除map中的filename
			System.out.println("mapRemove...");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	/*判断文件接收是否超时，如果超时则删除文件并记录日志*/
	public void run(){
		while (null != CreateHDFS.hdfsMap.get(fileName)) {//在文件还在读写阶段执行
			synchronized (CreateHDFS.hdfsMap) {
				Map<String, Object> map = CreateHDFS.hdfsMap.get(fileName);
				if (null!=map&&null != map.get("date")) {
					Date before = (Date) map.get("date");
					Date now = new Date();
					long l = now.getTime() - before.getTime();
					if (l > KafkaProperties.TIMEOUT * 1000) {//判断间隔时间是否超时
						deleteFile(map);//删除已经写入的文件
						String str=fileName+"  Timeout  At  " +now;
						writeLog(str,now,fileName,(String)map.get("specialId"),(String)map.get("mt"));//记录日志
						break;
					}
				}
			}
			try {
				Thread.sleep(20*1000);//每20秒检查一次
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
