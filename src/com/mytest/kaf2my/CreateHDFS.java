package com.mytest.kaf2my;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

public class CreateHDFS {
	
	public static Map<String,Map<String,Object>> hdfsMap=new Hashtable<String,Map<String,Object>>();
	public static FileSystem fileSystem=null;
	CountryUtil util=new CountryUtil();
	static{
		hdfsMap.put("past", new Hashtable<String, Object>());
		Configuration conf=new Configuration();
//		conf.set("fs.default.name", KafkaProperties.FSDEFAULTNAME);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.defaultFS", KafkaProperties.FSDEFAULTNAME);
		conf.set("dfs.nameservices", KafkaProperties.CLUSTERNAME);
		conf.set("dfs.ha.namenodes."+KafkaProperties.CLUSTERNAME, "nn1,nn2");
		conf.set("dfs.namenode.rpc-address."+KafkaProperties.CLUSTERNAME+".nn1", KafkaProperties.NN1);
		conf.set("dfs.namenode.rpc-address."+KafkaProperties.CLUSTERNAME+".nn2", KafkaProperties.NN2);
		conf.set("dfs.client.failover.proxy.provider."+KafkaProperties.CLUSTERNAME,org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.class.getName());
		try {
			fileSystem=FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@SuppressWarnings("unchecked")
	private  Map<String,Object> parseJson (String jsonStr) {//创建hdfs文件  
    	String json=jsonStr.replace(KafkaProperties.BETWEEN,",");
    	json=json.replace("\\n"," ");
    	json=json.replace(KafkaProperties.BEGIN," ");
   	    Map<String,Object> maps = (Map<String,Object>)JSON.parse(json);  
   	    /*for (Object map : maps.entrySet()){  
    		System.out.println(((Map.Entry)map).getKey()+"=[" + ((Map.Entry)map).getValue()+"]");  
    	}*/
    	return maps;
    } 
	
	
	public String getFilterStr(String str){//替换字符串中的\n和|
    	
		if( str != null ){
			str = str.replace(KafkaProperties.BETWEEN,",");
			str=str.replace("\\n"," ");
			str=str.replace(KafkaProperties.BEGIN," ");
		}
		return str;
    }
	/*通过接口得到新闻正文*/
	@SuppressWarnings("unchecked")
	public String getTextSrc(String uuid) throws Exception{
		String textSrc = "";
		HttpClient httpClient = new HttpClient();
		GetMethod getMethod = new GetMethod(KafkaProperties.INTERFACE+URLEncoder.encode("{\"nid\":\""+uuid+"\"}", "UTF-8"));
		getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,new DefaultHttpMethodRetryHandler());
		httpClient.executeMethod(getMethod);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		InputStream in = getMethod.getResponseBodyAsStream();
		int len = 0;
		byte[] buf = new byte[1024];
		while((len=in.read(buf))!=-1){
			out.write(buf, 0, len);
		}
		String jason = out.toString("UTF-8");
		if(null!=jason){
			Map<String,Object> maps=parseJson(jason);
			if(null!=maps.get("returndata")){
				Map<String,String> map=(Map<String,String>)maps.get("returndata");
				if(null!=map.get("textSrc")){
					textSrc=map.get("textSrc");
					textSrc=textSrc.replace(KafkaProperties.BETWEEN, ",");
				}
			}
		}
		getMethod.releaseConnection();
		return textSrc;
	}
	public boolean isEnglish(String charaString){//判断country是否是中文
        return charaString.matches("^[a-zA-Z ]*");

    }
	private Map<String,String> getStr (Map<String,Object> maps,String mt) throws Exception {    
		Map<String,String> strMap=new Hashtable<String,String>();
    	String str="";
    	if(mt.equals("soc")){//社交消息
    		//if (mid != null && !mid.isEmpty()){
			Integer  v_int;
			Long v_long;
			String specialId=(String)maps.get("specialId");
			String state=(String)maps.get("state");
			String totalCount=(String)maps.get("totalCount");
			String type=(String)maps.get("type");
			if("EOF".equals(state)||"ERROR".equals(state)){//判断是否是特殊标识消息
				strMap.put("specialId", specialId);
				strMap.put("state", state);
				strMap.put("totalCount", totalCount);
				strMap.put("type", type);
				return strMap;
			}
			
			int			atdCnt			= 0;
 			v_int 		= (Integer)maps.get("atdCnt");
			if( v_int != null)
 				 atdCnt = v_int.intValue();

 			String		city			=(String)maps.get("city");
 			
 			int			cityId			= 0;
 			v_int 		= (Integer)maps.get("cityId");
			if( v_int != null)
 				 cityId = v_int.intValue();

 			int			cmtCnt			= 0;
 			v_int 		= (Integer)maps.get("cmtCnt");
			if( v_int != null)
 				 cmtCnt = v_int.intValue();

 			String		country			=(String)maps.get("country");
 			String country_EN="";
 			String country_ZH="";
 			if(null!=country&&""!=country){//将传过来的country转换并存入country_EN和country_ZH
 				if(isEnglish(country)){
 					country_EN=country;
 					country_ZH=util.getCountry(country);
 				}else{
 					country_ZH=country;
 					country_EN=util.getCountry(country);
 				}
 			}

 			int			flwCnt			= 0;
 			v_int 		= (Integer)maps.get("flwCnt");
			if( v_int != null)
 				 flwCnt = v_int.intValue();

 			int			frdCnt			= 0;
 			v_int 		= (Integer)maps.get("frdCnt");
			if( v_int != null)
 				 frdCnt = v_int.intValue();

 			int			gender			= 0;
 			v_int 		= (Integer)maps.get("gender");
			if( v_int != null)
 				 gender = v_int.intValue();


 			String		id				=(String)maps.get("id");

 			int			isOri			= 0;
 			v_int 		= (Integer)maps.get("isOri");
			if( v_int != null)
 				 isOri = v_int.intValue();


 			String		languageCode	=(String)maps.get("languageCode");

 			String		myId			=(String)maps.get("myId");
 			
 			String		name			=(String)maps.get("name");
 			
 			String		province		=(String)maps.get("province");

 			int			provinceId		= 0;
 			v_int 		= (Integer)maps.get("provinceId");
			if( v_int != null)
 				provinceId  = v_int.intValue();

 			int			rpsCnt			= 0;
 			v_int 		= (Integer)maps.get("rpsCnt");
			if( v_int != null)
 				 rpsCnt = v_int.intValue();

			int			sentimentOrient	= 0;
 			v_int= (Integer)maps.get("sentimentOrient");
 			if(v_int != null)
 				sentimentOrient = v_int.intValue();

 			String		sourceType		=(String)maps.get("sourceType");

 			int			staCnt			= 0;
 			v_int= (Integer)maps.get("staCnt");
 			if(v_int != null)
 				staCnt = v_int.intValue();

 			String		text			=(String)maps.get("text");
 			if(text != null )	text = getFilterStr(text);

 			int			textLen			= 0;
 			v_int= (Integer)maps.get("textLen");
 			if(v_int != null)
 				textLen = v_int.intValue();

 			long		time			= 0;
 			try{
 				v_long= (Long)maps.get("time");
 	 			if(v_int != null)
 	 				time  = v_long.longValue();
 			}catch(Exception e){
 				Integer i=(Integer)maps.get("time");
 				time=i.longValue();
 			}

 			String		timeStr			=(String)maps.get("timeStr");
 			SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
 			SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
 			if(null!=timeStr&&""!=timeStr){
 				Date date=dateFormat.parse(timeStr);
 				timeStr=format.format(date);
 			}
 			
 			String		title			=(String)maps.get("title");
 			if(title != null )	title = getFilterStr(title);

 			String		url				=(String)maps.get("url");
 			
 			String		userId			=(String)maps.get("userId");
 			
 			String		userType		=(String)maps.get("userType");

 			int			verified		= 0;
 			v_int= (Integer)maps.get("verified");
 			if(v_int != null)
 				verified = v_int.intValue();
 			
 			long		view			= 0;
 			v_int= (Integer)maps.get("view");
 			if(v_int != null)
 				view = v_int.intValue();

    		str=KafkaProperties.BEGIN
    				+specialId				+KafkaProperties.BETWEEN
    				+atdCnt					+KafkaProperties.BETWEEN
    				+city					+KafkaProperties.BETWEEN
    				+cityId					+KafkaProperties.BETWEEN   	
    				+cmtCnt					+KafkaProperties.BETWEEN    
    				+country_EN				+KafkaProperties.BETWEEN
    				+country_ZH				+KafkaProperties.BETWEEN
    				+flwCnt					+KafkaProperties.BETWEEN    
    				+frdCnt					+KafkaProperties.BETWEEN   	
    				+gender					+KafkaProperties.BETWEEN   	
    				+id						+KafkaProperties.BETWEEN
    				+isOri					+KafkaProperties.BETWEEN   	
    				+languageCode			+KafkaProperties.BETWEEN
    				+myId					+KafkaProperties.BETWEEN
    				+name					+KafkaProperties.BETWEEN
    				+province				+KafkaProperties.BETWEEN
    				+provinceId				+KafkaProperties.BETWEEN    
    				+rpsCnt					+KafkaProperties.BETWEEN    
    				+sentimentOrient		+KafkaProperties.BETWEEN    
    				+sourceType				+KafkaProperties.BETWEEN
    				+staCnt					+KafkaProperties.BETWEEN    
    				+text					+KafkaProperties.BETWEEN
    				+textLen				+KafkaProperties.BETWEEN  	
    				+time					+KafkaProperties.BETWEEN    
    				+timeStr				+KafkaProperties.BETWEEN
    				+title					+KafkaProperties.BETWEEN
    				+url					+KafkaProperties.BETWEEN
    				+userId					+KafkaProperties.BETWEEN
    				+userType				+KafkaProperties.BETWEEN
    				+verified				+KafkaProperties.BETWEEN    
    				+view;
    		strMap.put("specialId", specialId);
    		strMap.put("str", str);
    	} else if( mt.equals("news") ){//新闻消息
			Integer  v_int;
			JSONArray	v_ja;
			String specialId=(String)maps.get("specialId");
			String state=(String)maps.get("state");
			String totalCount=(String)maps.get("totalCount");
			String type=(String)maps.get("type");
			if("EOF".equals(state)||"ERROR".equals(state)){//判断是否是特殊标识消息
				strMap.put("specialId", specialId);
				strMap.put("state", state);
				strMap.put("totalCount", totalCount);
				strMap.put("type", type);
				return strMap;
			}
			
    		String 	abstractZh		=(String)maps.get("abstractZh");
    		if(abstractZh != null )	abstractZh = getFilterStr(abstractZh);

    		String	countryNameEn	=(String)maps.get("countryNameEn");
    		
    		String	countryNameZh	=(String)maps.get("countryNameZh");
    		
    		String	districtNameEn	=(String)maps.get("districtNameEn");
    		
    		String	districtNameZh	=(String)maps.get("districtNameZh");

    		String	domain			=(String)maps.get("domain");

    		int	eventCountryId	= 0;      		//***int not long 
 			v_int 		= (Integer)maps.get("eventCountryId");
			if( v_int != null)
 				eventCountryId  = v_int.intValue();
			
			String keywordsEn="";
			try{
				v_ja		=(JSONArray)maps.get("keywordsEn");
				keywordsEn = v_ja.toString();
			}catch(Exception e){
				keywordsEn =(String)maps.get("keywordsEn");
			}
			
			String keywordsZh="";
			try{
				v_ja		=(JSONArray)maps.get("keywordsZh");
				keywordsZh = v_ja.toString();
			}catch(Exception e){
				keywordsZh =(String)maps.get("keywordsZh");
			}
			
    		String	languageTname	=(String)maps.get("languageTname");
    		
    		String	latitude		=(String)maps.get("latitude");

    		int		locationId		= 0;
 			v_int 		= (Integer)maps.get("locationId");
			if( v_int != null)
 				 locationId = v_int.intValue();
			
			String longitude=(String)maps.get("longitude");

    		int		mediaLevel		= 0;
 			v_int 		= (Integer)maps.get("mediaLevel");
			if( v_int != null)
 				 mediaLevel = v_int.intValue();

    		String	mediaNameSrc	=(String)maps.get("mediaNameSrc");
			
    		String	mediaTname		=(String)maps.get("mediaTname");

    		String	provinceNameEn	=(String)maps.get("provinceNameEn");
    		
    		String	provinceNameZh	=(String)maps.get("provinceNameZh");

    		String	pubdate			=(String)maps.get("pubdate");
    		SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
 			SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
 			if(null!=pubdate&&""!=pubdate){
 				Date date=dateFormat.parse(pubdate);
 				pubdate=format.format(date);
 			}

    		int		sensitiveType	= 0;
 			v_int 		= (Integer)maps.get("sensitiveType");
			if( v_int != null)
 				 sensitiveType = v_int.intValue();

    		int		sentimentId		= 0;
 			v_int 		= (Integer)maps.get("sentimentId");
			if( v_int != null)
 				 sentimentId = v_int.intValue();

			String tagId=(String)maps.get("tagId");
			
    		String	titleSrc		=(String)maps.get("titleSrc");
    		if(titleSrc != null )	titleSrc = getFilterStr(titleSrc);    		
			
    		int		transfer		= 0;
 			v_int 		= (Integer)maps.get("transfer");
			if( v_int != null)
 				 transfer = v_int.intValue();

    		String	url				=(String)maps.get("url");

    		String	uuid			=(String)maps.get("uuid");

    		int		view			= 0;
 			v_int 		= (Integer)maps.get("view");
			if( v_int != null)
 				 view = v_int.intValue();

			String	textSrc			="";
			if(null!=uuid){
				textSrc=getTextSrc(uuid);//取得接口中的正文
				if(null!=textSrc){
					textSrc=getFilterStr(textSrc);
				}
			}

    		str=KafkaProperties.BEGIN
    				+specialId			+KafkaProperties.BETWEEN
    				+abstractZh	  		+KafkaProperties.BETWEEN
    				+countryNameEn		+KafkaProperties.BETWEEN
    				+countryNameZh		+KafkaProperties.BETWEEN
    				+districtNameEn 	+KafkaProperties.BETWEEN
    				+districtNameZh 	+KafkaProperties.BETWEEN
    				+domain		  		+KafkaProperties.BETWEEN
    				+eventCountryId   	+KafkaProperties.BETWEEN    
    				+keywordsEn	  		+KafkaProperties.BETWEEN
    				+keywordsZh	  		+KafkaProperties.BETWEEN
    				+languageTname		+KafkaProperties.BETWEEN
    				+latitude	  		+KafkaProperties.BETWEEN
    				+locationId	      	+KafkaProperties.BETWEEN
    				+longitude			+KafkaProperties.BETWEEN
    				+mediaLevel	      	+KafkaProperties.BETWEEN    
    				+mediaNameSrc 		+KafkaProperties.BETWEEN
    				+mediaTname	  		+KafkaProperties.BETWEEN
    				+provinceNameEn 	+KafkaProperties.BETWEEN
    				+provinceNameZh 	+KafkaProperties.BETWEEN
    				+pubdate	  		+KafkaProperties.BETWEEN
    				+sensitiveType	  	+KafkaProperties.BETWEEN    
    				+sentimentId	  	+KafkaProperties.BETWEEN
    				+tagId				+KafkaProperties.BETWEEN
    				+textSrc	  		+KafkaProperties.BETWEEN
    				+titleSrc	  		+KafkaProperties.BETWEEN
    				+transfer		  	+KafkaProperties.BETWEEN    
    				+url		  		+KafkaProperties.BETWEEN
    				+uuid		  		+KafkaProperties.BETWEEN
    				+view;
    		strMap.put("specialId", specialId);
    		strMap.put("str", str);
    	}else{
    		System.out.println("MsgType=["+mt+"] not support now");
    	}
//    	System.out.println(mt+str);
    	return strMap;
    }
	/*写文件*/
	public void write (String fileName,String str, Date date,String specialId,String mt) throws Exception {
		if(hdfsMap.containsKey(fileName)){//继续写
			Map<String,Object> map=hdfsMap.get(fileName);//拿到map
			if(null!=map&&null!=map.get("size")&&(Integer)map.get("size")>1024*1024*KafkaProperties.SIZE){//判断文件是否超过指定大小
				changeSuffix(fileName,str, map, date);//如果超过则更换文件
			}else{
				continueWrite(str, map, date);//如果没超过，则继续写
			}
		}else{//生成第一个文件，并把新生成的输出流保存到hdfsMap
//			Configuration conf=new Configuration();
//			conf.set("fs.default.name", KafkaProperties.FSDEFAULTNAME);
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			FileSystem fileSystem=FileSystem.get(conf);
//			System.out.println("configue...");
			FSDataOutputStream outPutStream=fileSystem.create(new Path(fileName+"1.dat"));//生成第一个文件
			System.out.println("create...");
			String s=str.substring(KafkaProperties.BEGIN.length());//文件首条记录去掉分隔符
			outPutStream.write(s.getBytes("UTF-8"));
			System.out.println("write...");
//			outPutStream.flush();
			Map<String,Object> map=new Hashtable<String,Object>();//将本次读写信息存入map中
//			map.put("fileSystem", fileSystem);
			map.put("outPutStream", outPutStream);
			map.put("date", date);
			map.put("specialId", specialId);
			map.put("mt", mt);
			map.put("suffix", 1);//文件名后缀
			map.put("size", s.getBytes("UTF-8").length);
			map.put("names",fileName+"1.dat");//拼接文件名
			hdfsMap.put(fileName, map);
			TimeOutJudge judge=new TimeOutJudge(fileName);
			judge.start();//判断是否接收超时
		}
	}
	/*文件大于指定长度则更换文件名后缀*/
	public void changeSuffix(String fileName,String str,Map<String,Object> map,Date date) throws Exception{
//		FileSystem fileSystem=(FileSystem)map.get("fileSystem");
		FSDataOutputStream outPutStream=(FSDataOutputStream)map.get("outPutStream");
		if(null!=outPutStream){//取出并关闭当前输出流
			outPutStream.flush();
			outPutStream.close();
			System.out.println("streamClose...");
		}
//		if(null!=fileSystem){//关闭文件系统
//			fileSystem.close();
//			System.out.println("fileSystemClose...");
//		}
		int suffix=(Integer)map.get("suffix");
		map.put("suffix", ++suffix);//文件后缀自增
		map.put("names", (String)map.get("names")+"&&"+fileName+suffix+".dat");//拼接新的文件名
//		Configuration conf=new Configuration();
//		conf.set("fs.default.name", KafkaProperties.FSDEFAULTNAME);
//		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		FileSystem fs=FileSystem.get(conf);
		System.out.println("configue...");
		FSDataOutputStream ops=fileSystem.create(new Path(fileName+suffix+".dat"));//打开新的文件
		System.out.println("create...");
		String s=str.substring(KafkaProperties.BEGIN.length());//去掉文件开头的分隔符
		ops.write(s.getBytes("UTF-8"));
		System.out.println("write...");
		map.put("date", date);//更新最后读写时间
		map.put("size", s.getBytes("UTF-8").length);//重置文件大小
		map.put("outPutStream", ops);
	}
	public void continueWrite(String str,Map<String,Object> map,Date date) throws Exception{
		if(null!=map.get("outPutStream")){//取得保存好的输出流
			FSDataOutputStream outPutStream=(FSDataOutputStream)map.get("outPutStream");
			outPutStream.write(str.getBytes("UTF-8"));//追加
//			System.out.println("append...");
//			outPutStream.flush();
			int size=(Integer)map.get("size");
			size+=str.getBytes().length;
			map.put("date", date);//更新文件最后读写时间
			map.put("size", size);//更新文件大小
		}
	}
	/*结束写入过程*/
	public void writeDone(String fileName,String specialId,String mt,String outPutDir) throws Exception{
		if(hdfsMap.containsKey(fileName)){
			Map<String,Object> map=hdfsMap.get(fileName);
//			FileSystem fileSystem=(FileSystem)map.get("fileSystem");
			FSDataOutputStream outPutStream=(FSDataOutputStream)map.get("outPutStream");
			if(null!=outPutStream){//取出并关闭输出流
				outPutStream.flush();
				outPutStream.close();
				System.out.println("streamClose...");
			}
//			if(null!=fileSystem){//关闭文件系统
//				fileSystem.close();
//				System.out.println("fileSystemClose...");
//			}
//			String names=(String)map.get("names");//取得所有文件名
			Map<String, Object> pastMap=(Map<String, Object>)CreateHDFS.hdfsMap.get("past");
			pastMap.put(fileName, new Date());//将完成的任务记录下来并记录时间
			writeLog(outPutDir,specialId,mt,0);//记录日志
			System.out.println("log...");
			hdfsMap.remove(fileName);//将用过的信息删除
			System.out.println("mapRemove...");
		}else{
			writeLog(outPutDir,specialId,mt,2);//如果没有数据直接记录日志
		}
	}
	/*写入日志文件*/
	public static void writeLog(String fileName,String specialId,String mt,int flag) throws Exception{
//		Configuration conf=new Configuration();
//		conf.set("fs.default.name", KafkaProperties.FSDEFAULTNAME);
//		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		FileSystem fileSystem=FileSystem.get(conf);
		String logName=KafkaProperties.LOGSDIR+"/"+mt+specialId+".log";//日志文件名
		FSDataOutputStream outPutStream=fileSystem.create(new Path(logName));
		String s=flag+"|"+fileName;//要写入文件的内容
		outPutStream.write(s.getBytes("UTF-8"));
		outPutStream.flush();
		outPutStream.close();
		SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
		PrintStream log = new PrintStream("./logs/"+specialId+".txt");//记录到项目本地的日志文件名
		PrintStream out = System.out;
		System.setOut(log);
		String detail="";
		if(0==flag){
			detail="Normally";
		}else if(1==flag){
			detail="TimeOut";
		}else if(2==flag){
			detail="NoneSource";
		}else if(3==flag){
			detail="TransferError";
		}
		String str=format.format(new Date())+"   "+mt+"   "+specialId+"   "+flag+"   "+detail;
		System.out.println(str);
		System.setOut(out);
//		fileSystem.close();
	}
	/*调用*/
	public void doHDFS(String msg,Date date,String mt) throws Exception{
		Map<String,Object> map = parseJson(msg);//将json数据放入map中
		String userId="";//用户id
		String taskId="";//任务id
		String str="";//即将写入的数据
		if(null!=map){
			Map<String,String> strMap=getStr(map,mt);//解析jason数据
			String specialId="";
			if(null!=strMap){
				specialId=strMap.get("specialId");//解析specialId
				String[] ids=specialId.split("-");
				if(null!=ids){
					taskId=ids[0];//任务id
					userId=ids[1];//用户id
				}
				if(!"".equals(userId)&&!"".equals(taskId)&&!"".equals(mt)){//任务id，用户id，消息类型均不能为空
					String fileName=KafkaProperties.SOURCEDIR+"/"+mt+"/"+userId+"/"+taskId+"/00";//生成文件名
					String outPutDir=KafkaProperties.SOURCEDIR+"/"+mt+"/"+userId+"/"+taskId+"/";//存入日志中的路径
					Map<String, Object> pastMap=(Map<String, Object>)hdfsMap.get("past");//取到读取完成的文件名
					if(pastMap.containsKey(fileName)){//文件生成就不再重新打开了
						System.out.println(fileName+" has arready finish writing !!!");
					}else if(null!=strMap.get("state")){//判断是否是标识数据
						if("EOF".equals(strMap.get("state"))){//完成状态
							writeDone(fileName,specialId,mt,outPutDir);//接到结束标识结束文件读写
						}else if("ERROR".equals(strMap.get("state"))){//异常状态
							//异常处理，内容待定
							writeLog(outPutDir,specialId,mt,3);//如果没有数据直接记录日志
						}
					}else{
						str=strMap.get("str");//拿到要书写的字符串
						write(fileName,str,date,specialId,mt);//向文件中写文件
					}
				}
			}
		}
	}
}
