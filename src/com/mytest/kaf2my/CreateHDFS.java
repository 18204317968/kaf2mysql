package com.mytest.kaf2my;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

public class CreateHDFS {
	
	private static String newsFileName;
	private static String socFileName;
	private static int newsFlag;
	private static int socFlag;
	public static FSDataOutputStream newsOutputStream;
	public static FSDataOutputStream socOutputStream;
	public static FileSystem newsHdfs;
	public static FileSystem socHdfs;
	
	private  Map parseJson (String jsonStr) {//创建hdfs文件  
    	
   	    Map maps = (Map)JSON.parse(jsonStr);  
   	    /*for (Object map : maps.entrySet()){  
    		System.out.println(((Map.Entry)map).getKey()+"=[" + ((Map.Entry)map).getValue()+"]");  
    	}*/
    	return maps;
    } 
	
	public void newsWriteHDFS (String filename,String str){//创建hdfs文件，并向文件中写入数据
		Configuration conf=new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.59.37:9000");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		try {
			if(newsFlag==1){
				Thread.sleep(100);
				newsOutputStream.close();
				newsHdfs.close();
			}
			String s=str.substring(3);
			newsHdfs=FileSystem.get(conf);
			System.out.println("connectNews......");
			Path path=new Path(filename);
			newsOutputStream=newsHdfs.create(path);
			System.out.println("createNewsFile......");
			newsOutputStream.write(s.getBytes("UTF-8"));
			System.out.println("writeUTF......");
			newsOutputStream.flush();
			System.out.println("flush......");
			newsFlag=1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void socWriteHDFS (String filename,String str){//创建hdfs文件，并向文件中写入数据
		Configuration conf=new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.59.37:9000");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		try {
			if(socFlag==1){
				Thread.sleep(100);
				socOutputStream.close();
				socHdfs.close();
			}
			String s=str.substring(3);
			socHdfs=FileSystem.get(conf);
			System.out.println("connectSoc......");
			Path path=new Path(filename);
			socOutputStream=socHdfs.create(path);
			System.out.println("createSocFile......");
			socOutputStream.write(s.getBytes("UTF-8"));
			System.out.println("writeUTF......");
			socOutputStream.flush();
			System.out.println("flush......");
			socFlag=1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void newsContinueWrite(String str){//不关闭输出流继续向文件写
		try{
			newsOutputStream.write(str.getBytes("UTF-8"));
			newsOutputStream.flush();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void socContinueWrite(String str){//不关闭输出流继续向文件写
		try{
			socOutputStream.write(str.getBytes("UTF-8"));
			socOutputStream.flush();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public String getFilterStr(String str){//替换字符串中的\和'
    	
		if( str != null ){
			str = str.replace("\"", "\\\"");
			str = str.replace("'", "");
		}
		return str;
    }
	private  String getStr (Map maps,String mt) {    
    	
    	String str="";
    	if(mt.equals("soc")){//社交消息
    		//if (mid != null && !mid.isEmpty()){
    		
			Integer  v_int;
			Long v_long;
			
			int			atdCnt			= 0;
 			v_int 		= (Integer)maps.get("atdCnt");
			if( v_int != null)
 				 atdCnt = v_int.intValue();
//			System.out.println("atdCnt="+atdCnt);    


 			String		city			=(String)maps.get("city");
//			System.out.println("city="+city);
 			
 			int			cityId			= 0;
 			v_int 		= (Integer)maps.get("cityId");
			if( v_int != null)
 				 cityId = v_int.intValue();
//			System.out.println("cityId="+cityId);   	


 			int			cmtCnt			= 0;
 			v_int 		= (Integer)maps.get("cmtCnt");
			if( v_int != null)
 				 cmtCnt = v_int.intValue();
//			System.out.println("cmtCnt="+cmtCnt);    


 			long		commentSince	= 0;
 			v_int 		= (Integer)maps.get("commentSince");
			if( v_int != null)
 				 commentSince = v_int.intValue();
//			System.out.println("commentSince="+commentSince);    

 			
 			String		companies		="";
 			try{
 				companies=(String)maps.get("companies");
 			}catch(Exception e){
 				JSONArray j=(JSONArray)maps.get("companies");
 				companies=j.toString();
 			}
//			System.out.println("companies="+companies);
			
 			String		country			=(String)maps.get("country");
//			System.out.println("country="+country);

 			int			delFlag			= 0;
 			v_int 		= (Integer)maps.get("delFlag");
			if( v_int != null)
 				delFlag  = v_int.intValue();
//			System.out.println("delFlag="+delFlag);    


 			String		delReason		=(String)maps.get("delReason");
//			System.out.println("delReason="+delReason);

 			long		eventCountryId	= 0;
 			v_int 		= (Integer)maps.get("eventCountryId");
			if( v_int != null)
 				 eventCountryId = v_int.intValue();
//			System.out.println("eventCountryId="+eventCountryId);    


 			long		eventDistrictId	= 0;
 			v_int 		= (Integer)maps.get("eventDistrictId");
			if( v_int != null)
 				 eventDistrictId = v_int.intValue();
//			System.out.println("eventDistrictId="+eventDistrictId);    

 			String		eventElement	=(String)maps.get("eventElement");
//			System.out.println("eventElement="+eventElement);
 			String		eventId			=(String)maps.get("eventId");
//			System.out.println("eventId="+eventId);

 			long		eventProvinceId	= 0;
 			v_int 		= (Integer)maps.get("eventProvinceId");
			if( v_int != null)
 				 eventProvinceId = v_int.intValue();
//			System.out.println("eventProvinceId="+eventProvinceId);    

 			int			flwCnt			= 0;
 			v_int 		= (Integer)maps.get("flwCnt");
			if( v_int != null)
 				 flwCnt = v_int.intValue();
//			System.out.println("flwCnt="+flwCnt);    

 			int			frdCnt			= 0;
 			v_int 		= (Integer)maps.get("frdCnt");
			if( v_int != null)
 				 frdCnt = v_int.intValue();
//			System.out.println("frdCnt="+frdCnt);   	

 			int			gender			= 0;
 			v_int 		= (Integer)maps.get("gender");
			if( v_int != null)
 				 gender = v_int.intValue();
//			System.out.println("gender="+gender);   	


 			String		id				=(String)maps.get("id");
//			System.out.println("id="+id);

 			int			isOri			= 0;
 			v_int 		= (Integer)maps.get("isOri");
			if( v_int != null)
 				 isOri = v_int.intValue();
//			System.out.println("isOri="+isOri);   	


 			String		languageCode	=(String)maps.get("languageCode");
//			System.out.println("languageCode="+languageCode);
 			String		latitude		=(String)maps.get("latitude");
//			System.out.println("latitude="+latitude);

 			long		locationId		= 0;
 			v_int 		= (Integer)maps.get("locationId");
			if( v_int != null)
 				 locationId = v_int.intValue();
//			System.out.println("locationId="+locationId);   	


 			String		bigintitude		=(String)maps.get("bigintitude");
//			System.out.println("bigintitude="+bigintitude);
 			String		myId			=(String)maps.get("myId");
//			System.out.println("myId="+myId);
 			String		name			=(String)maps.get("name");
//			System.out.println("name="+name);
 			String		products		="";
 			try{
 				products=(String)maps.get("products");
 			}catch(Exception e){
 				JSONArray j=(JSONArray)maps.get("products");
 				products=j.toString();
 			}
//			System.out.println("products="+products);
 			String		province		=(String)maps.get("province");
//			System.out.println("province="+province);

 			int			provinceId		= 0;
 			v_int 		= (Integer)maps.get("provinceId");
			if( v_int != null)
 				provinceId  = v_int.intValue();
//			System.out.println("provinceId="+provinceId);    


 			String		pubAffairKeyword=(String)maps.get("pubAffairKeyword");
//			System.out.println("pubAffairKeyword="+pubAffairKeyword);

 			int			pubAffairType	= 0;
 			v_int 		= (Integer)maps.get("pubAffairType");
			if( v_int != null)
 				 pubAffairType = v_int.intValue();
//			System.out.println("pubAffairType="+pubAffairType);    


 			long		repostSince		= 0;
 			v_int 		= (Integer)maps.get("repostSince");
			if( v_int != null)
 				 repostSince = v_int.longValue();
//			System.out.println("repostSince="+repostSince);    


 			int			rpsCnt			= 0;
 			v_int 		= (Integer)maps.get("rpsCnt");
			if( v_int != null)
 				 rpsCnt = v_int.intValue();
//			System.out.println("rpsCnt="+rpsCnt);    
 			
			/*
 			int			sentiment		= 0; 
 			String v_str= (String)maps.get("sentiment");
 			if(v_str != null && !v_str.isEmpty())
 				sentiment = (new Integer(v_str)).intValue();
 			*/	
 			
 			String		sentiment		=(String)maps.get("sentiment");
//			System.out.println("sentiment_XXXXX="+sentiment);
 			
 			int			sentimentOrient	= 0;
 			v_int= (Integer)maps.get("sentimentOrient");
 			if(v_int != null)
 				sentimentOrient = v_int.intValue();
//			System.out.println("sentimentOrient="+sentimentOrient);    

 			String		sourceType		=(String)maps.get("sourceType");
//			System.out.println("sourceType="+sourceType);
 			String		sourceWeiboId	=(String)maps.get("sourceWeiboId");
//			System.out.println("sourceWeiboId="+sourceWeiboId);
 			String		sourceWeiboMyId	=(String)maps.get("sourceWeiboMyId");
//			System.out.println("sourceWeiboMyId="+sourceWeiboMyId);

 			int			staCnt			= 0;
 			v_int= (Integer)maps.get("staCnt");
 			if(v_int != null)
 				staCnt = v_int.intValue();
//			System.out.println("staCnt="+staCnt);    

 			String		text			=(String)maps.get("text");
 			if(text != null )	text = getFilterStr(text);
//			System.out.println("text="+text);

 			String		textEn			=(String)maps.get("textEn");
 			if(textEn != null )	textEn = getFilterStr(textEn);
//			System.out.println("textEn="+textEn);

 			int			textLen			= 0;
 			v_int= (Integer)maps.get("textLen");
 			if(v_int != null)
 				textLen = v_int.intValue();
//			System.out.println("textLen="+textLen);  	

 			String		textZh			=(String)maps.get("textZh");
//			System.out.println("textZh="+textZh);

 			long		time			= 0;
 			v_long= (Long)maps.get("time");
 			if(v_int != null)
 				time  = v_long.longValue();

//			System.out.println("time="+time);    

 			String		timeStr			=(String)maps.get("timeStr");
//			System.out.println("timeStr="+timeStr);
 			String		title			=(String)maps.get("title");
 			if(title != null )	title = getFilterStr(title);
//			System.out.println("title="+title);

 			long		updateTime		= 0;
 			v_long= (Long)maps.get("updateTime");
 			if(v_long != null)
 				updateTime = v_long.longValue();
//			System.out.println("updateTime="+updateTime);   	


 			String		updateTimeStr	=(String)maps.get("updateTimeStr");
//			System.out.println("updateTimeStr="+updateTimeStr);
 			String		url				=(String)maps.get("url");
//			System.out.println("url="+url);
 			String		userAvatar		=(String)maps.get("userAvatar");
//			System.out.println("userAvatar="+userAvatar);
 			String		userId			=(String)maps.get("userId");
//			System.out.println("userId="+userId);
 			String		userTag			=(String)maps.get("userTag");
//			System.out.println("userTag="+userTag);
 			String		userType		=(String)maps.get("userType");
//			System.out.println("userType="+userType);

 			int			verified		= 0;
 			v_int= (Integer)maps.get("verified");
 			if(v_int != null)
 				verified = v_int.intValue();
//			System.out.println("verified="+verified);    
 			
 			String		verifiedReason	=(String)maps.get("verifiedReason");
//			System.out.println("verifiedReason="+verifiedReason);

 			long		view			= 0;
 			v_int= (Integer)maps.get("view");
 			if(v_int != null)
 				view = v_int.intValue();
//			System.out.println("view="+view);    

    		str=KafkaProperties.BEGIN
    				+ atdCnt				+KafkaProperties.BETWEEN
    				+ city					+KafkaProperties.BETWEEN
    				+ cityId				+KafkaProperties.BETWEEN   	
    				+ cmtCnt				+KafkaProperties.BETWEEN    
    				+ commentSince			+KafkaProperties.BETWEEN    
    				+ companies				+KafkaProperties.BETWEEN
    				+ country				+KafkaProperties.BETWEEN
    				+ delFlag				+KafkaProperties.BETWEEN 
    				+ delReason				+KafkaProperties.BETWEEN
    				+ eventCountryId		+KafkaProperties.BETWEEN    
    				+ eventDistrictId		+KafkaProperties.BETWEEN    
    				+eventElement			+KafkaProperties.BETWEEN
    				+eventId				+KafkaProperties.BETWEEN
    				+ eventProvinceId		+KafkaProperties.BETWEEN    
    				+ flwCnt				+KafkaProperties.BETWEEN    
    				+ frdCnt				+KafkaProperties.BETWEEN   	
    				+ gender				+KafkaProperties.BETWEEN   	
    				+id						+KafkaProperties.BETWEEN
    				+ isOri					+KafkaProperties.BETWEEN   	
    				+languageCode			+KafkaProperties.BETWEEN
    				+latitude				+KafkaProperties.BETWEEN
    				+ locationId			+KafkaProperties.BETWEEN   	
    				+bigintitude			+KafkaProperties.BETWEEN
    				+myId					+KafkaProperties.BETWEEN
    				+name					+KafkaProperties.BETWEEN
    				+products				+KafkaProperties.BETWEEN
    				+province				+KafkaProperties.BETWEEN
    				+ provinceId			+KafkaProperties.BETWEEN    
    				+pubAffairKeyword		+KafkaProperties.BETWEEN
    				+ pubAffairType			+KafkaProperties.BETWEEN    
    				+ repostSince			+KafkaProperties.BETWEEN    
    				+ rpsCnt				+KafkaProperties.BETWEEN    
    				+sentiment				+KafkaProperties.BETWEEN    
    				+ sentimentOrient		+KafkaProperties.BETWEEN    
    				+sourceType				+KafkaProperties.BETWEEN
    				+sourceWeiboId			+KafkaProperties.BETWEEN
    				+sourceWeiboMyId		+KafkaProperties.BETWEEN
    				+staCnt				+KafkaProperties.BETWEEN    
    				+text					+KafkaProperties.BETWEEN
    				+textEn					+KafkaProperties.BETWEEN
    				+ textLen				+KafkaProperties.BETWEEN  	
    				+textZh					+KafkaProperties.BETWEEN
    				+ time					+KafkaProperties.BETWEEN    
    				+timeStr				+KafkaProperties.BETWEEN
    				+title					+KafkaProperties.BETWEEN
    				+updateTime				+KafkaProperties.BETWEEN   	
    				+updateTimeStr			+KafkaProperties.BETWEEN
    				+url					+KafkaProperties.BETWEEN
    				+userAvatar				+KafkaProperties.BETWEEN
    				+userId					+KafkaProperties.BETWEEN
    				+userTag				+KafkaProperties.BETWEEN
    				+userType				+KafkaProperties.BETWEEN
    				+verified				+KafkaProperties.BETWEEN    
    				+verifiedReason			+KafkaProperties.BETWEEN
    				+view;
    	} else if( mt.equals("news") ){//新闻消息
    		
    	//if( uuid!=null && !uuid.isEmpty() ){
  
			Integer  v_int;
			Long v_long;
			JSONArray	v_ja;

			String 	abstractEn		=(String)maps.get("abstractEn");    
			if(abstractEn != null )	abstractEn = getFilterStr(abstractEn);
//			System.out.println("abstractEn		="+abstractEn		);
			
    		String 	abstractZh		=(String)maps.get("abstractZh");
    		if(abstractZh != null )	abstractZh = getFilterStr(abstractZh);
//			System.out.println("abstractZh		="+abstractZh		);
			
    		String 	author			=(String)maps.get("author");
//			System.out.println("author			="+author			);

    		int 	categoryId		= 0;
 			v_int 		= (Integer)maps.get("categoryId");
			if( v_int != null)
 				categoryId  = v_int.intValue();
//			System.out.println("categoryId		="+categoryId		);


    		String	comeFrom		=(String)maps.get("comeFrom");
//			System.out.println("comeFrom		="+comeFrom			);
			
    		String	comeFromDb		=(String)maps.get("comeFromDb");
//			System.out.println("comeFromDb		="+comeFromDb		);
			
			v_ja		=(JSONArray)maps.get("companies");
			String companies = v_ja.toString();
//			System.out.println("companies		="+companies		);
			
    		long	countryId		= 0;
 			v_long 		= (Long)maps.get("countryId");
			if( v_long != null)
 				 countryId = v_long.longValue();
//			System.out.println("countryId		="+countryId		);

    		String	countryNameEn	=(String)maps.get("countryNameEn");
//			System.out.println("countryNameEn	="+countryNameEn	);
    		String	countryNameZh	=(String)maps.get("countryNameZh");
//			System.out.println("countryNameZh	="+countryNameZh	);
    		String	created			=(String)maps.get("created");
//			System.out.println("created			="+created			);

    		int		delFlag			= 0;
 			v_int 		= (Integer)maps.get("delFlag");
			if( v_int != null)
 				 delFlag = v_int.intValue();
//			System.out.println("delFlag			="+delFlag			);


    		String	delReason		=(String)maps.get("delReason");
//			System.out.println("delReason		="+delReason		);

    		long	districtId		= 0;
 			v_long 		= (Long)maps.get("districtId");
			if( v_long != null)
 				 districtId = v_int.longValue();
//			System.out.println("districtId		="+districtId		);


    		String	districtNameEn	=(String)maps.get("districtNameEn");
//			System.out.println("districtNameEn	="+districtNameEn	);
    		String	districtNameZh	=(String)maps.get("districtNameZh");
//			System.out.println("districtNameZh	="+districtNameZh	);

    		int		docLength		= 0;
 			v_int 		= (Integer)maps.get("docLength");
			if( v_int != null)
 				docLength  = v_int.intValue();
//			System.out.println("docLength		="+docLength		);


    		String	domain			=(String)maps.get("domain");
//			System.out.println("domain			="+domain			);

    		int		domainId		= 0;
 			v_int 		= (Integer)maps.get("domainId");
			if( v_int != null)
 				 domainId = v_int.intValue();
//			System.out.println("domainId		="+domainId			);


    		int	eventCountryId	= 0;      		//***int not long 
 			v_int 		= (Integer)maps.get("eventCountryId");
			if( v_int != null)
 				eventCountryId  = v_int.intValue();
//			System.out.println("eventCountryId	="+eventCountryId	);

    		int	eventDistrictId	= 0;    		//***int not long 
 			v_int 		= (Integer)maps.get("eventDistrictId");
			if( v_int != null)
 				 eventDistrictId = v_int.intValue();
//			System.out.println("eventDistrictId	="+eventDistrictId	);


    		String	eventElement	=(String)maps.get("eventElement");
//			System.out.println("eventElement	="+eventElement		);
    		String	eventId			=(String)maps.get("eventId");
//			System.out.println("eventId			="+eventId			);

    		int		eventProvinceId	= 0;
 			v_int 		= (Integer)maps.get("eventProvinceId");
			if( v_int != null)
 				 eventProvinceId = v_int.intValue();
//			System.out.println("eventProvinceId	="+eventProvinceId	);

    		int		isHome			= 0;
 			String s_isHome 		= (String)maps.get("isHome");
			if( s_isHome != null)
 				 isHome = (new Integer(s_isHome)).intValue();
//			System.out.println("isHome			="+isHome			);

    		int		isOriginal		= 0;
 			String s_iso 		= (String)maps.get("isOriginal");
			if( s_iso != null)
 				 isOriginal = (new Integer(s_iso)).intValue();
//			System.out.println("isOriginal		="+isOriginal		);

    		int		isPicture		= 0;
 			String s_isp 		= (String)maps.get("isPicture");
			if( s_isp != null)
 				 isPicture = (new Integer(s_isp)).intValue();
//			System.out.println("isPicture		="+isPicture		);


    		int		isSensitive		= 0;
 			v_int 		= (Integer)maps.get("isSensitive");
			if( v_int != null)
 				 isSensitive = v_int.intValue();
//			System.out.println("isSensitive		="+isSensitive		);
			
			v_ja		=(JSONArray)maps.get("keywordsEn");
			String keywordsEn = v_ja.toString();
//			System.out.println("keywordsEn		="+keywordsEn		);
			
			
			v_ja		=(JSONArray)maps.get("keywordsZh");
    		String	keywordsZh		=v_ja.toString();
//			System.out.println("keywordsZh		="+keywordsZh		);
			
    		String	languageCode	=(String)maps.get("languageCode");
//			System.out.println("languageCode	="+languageCode		);
    		String	languageTname	=(String)maps.get("languageTname");
//			System.out.println("languageTname	="+languageTname	);
    		String	latitude		=(String)maps.get("latitude");
//			System.out.println("latitude		="+latitude			);

    		int		locationId		= 0;
 			v_int 		= (Integer)maps.get("locationId");
			if( v_int != null)
 				 locationId = v_int.intValue();
//			System.out.println("locationId		="+locationId		);

    		String	bigintitude		=(String)maps.get("bigintitude");
//			System.out.println("bigintitude		="+bigintitude		);

    		int		mediaLevel		= 0;
 			v_int 		= (Integer)maps.get("mediaLevel");
			if( v_int != null)
 				 mediaLevel = v_int.intValue();
//			System.out.println("mediaLevel		="+mediaLevel		);

    		String	mediaNameEn		=(String)maps.get("mediaNameEn");
    		if(mediaNameEn != null )	mediaNameEn = getFilterStr(mediaNameEn);    		
//			System.out.println("mediaNameEn		="+mediaNameEn		);
			
    		String	mediaNameSrc	=(String)maps.get("mediaNameSrc");
//			System.out.println("mediaNameSrc	="+mediaNameSrc		);
			
    		String	mediaNameZh		=(String)maps.get("mediaNameZh");
//			System.out.println("mediaNameZh		="+mediaNameZh		);


    		String	mediaTname		=(String)maps.get("mediaTname");
    		
//			System.out.println("mediaTname		="+mediaTname		);

    		int		mediaType		= 0;
 			v_int 		= (Integer)maps.get("mediaType");
			if( v_int != null)
 				 mediaType = v_int.intValue();
//			System.out.println("mediaType		="+mediaType		);
			
			v_ja		=(JSONArray)maps.get("products");
    		String	products		=v_ja.toString();
//			System.out.println("products		="+products			);

    		long	provinceId		= 0;
    		v_long 		= (Long)maps.get("provinceId");
			if( v_long != null)
				provinceId = v_long.longValue();
//			System.out.println("provinceId		="+provinceId		);

    		String	provinceNameEn	=(String)maps.get("provinceNameEn");
//			System.out.println("provinceNameEn	="+provinceNameEn	);
    		String	provinceNameZh	=(String)maps.get("provinceNameZh");
//			System.out.println("provinceNameZh	="+provinceNameZh	);
    		String	pubAffairKeyword=(String)maps.get("pubAffairKeyword");
//			System.out.println("pubAffairKeyword="+pubAffairKeyword	);

    		int		pubAffairType	= 0;
 			v_int 		= (Integer)maps.get("pubAffairType");
			if( v_int != null)
 				 pubAffairType = v_int.intValue();
//			System.out.println("pubAffairType	="+pubAffairType	);

    		String	pubdate			=(String)maps.get("pubdate");
//			System.out.println("pubdate			="+pubdate			);

    		long	pubTime			= 0;
    		try{
    			pubTime=(Long)maps.get("pubTime");
    		}catch(Exception e){
    			Integer i=(Integer)maps.get("pubTime");
    			pubTime=i.longValue();
    		}
//			System.out.println("pubTime			="+pubTime			);

    		int		pv				=((Integer)maps.get("pv")).intValue();    
 			v_int 		= (Integer)maps.get("pv");
			if( v_int != null)
 				pv  = v_int.intValue();
//			System.out.println("pv				="+pv				);

    		String	sensitiveCls	=(String)maps.get("sensitiveCls");
//			System.out.println("sensitiveCls	="+sensitiveCls		);

    		int		sensitiveType	= 0;
 			v_int 		= (Integer)maps.get("sensitiveType");
			if( v_int != null)
 				 sensitiveType = v_int.intValue();
//			System.out.println("sensitiveType	="+sensitiveType	);

    		int		sentimentId		= 0;
 			v_int 		= (Integer)maps.get("sentimentId");
			if( v_int != null)
 				 sentimentId = v_int.intValue();
//			System.out.println("sentimentId		="+sentimentId		);

    		String	similarityId	=(String)maps.get("similarityId");
//			System.out.println("similarityId	="+similarityId		);
    		String	textEn			=(String)maps.get("textEn");
//			System.out.println("textEn			="+textEn			);

    		long	textQuality		= 0;
    		v_long 		= (Long)maps.get("textQuality");
			if( v_long != null)
 				 textQuality = v_long.longValue();
//			System.out.println("textQuality		="+textQuality		);

    		String	textSrc			=(String)maps.get("textSrc");
    		if(textSrc != null )	textSrc = getFilterStr(textSrc);
//    		System.out.println("textSrc			="+textSrc			);
    		
    		String	textZh			=(String)maps.get("textZh");
    		if(textZh != null )	textZh = getFilterStr(textZh);
//			System.out.println("textZh			="+textZh			);
			
    		String	titleEn			=(String)maps.get("titleEn");
    		if(titleEn != null )	titleEn = getFilterStr(titleEn);   		
//			System.out.println("titleEn			="+titleEn			);
			
    		String	titleSrc		=(String)maps.get("titleSrc");
    		if(titleSrc != null )	titleSrc = getFilterStr(titleSrc);    		
//			System.out.println("titleSrc		="+titleSrc			);
			
    		String	titleZh			=(String)maps.get("titleZh");
    		if(titleZh != null )	titleZh = getFilterStr(titleZh);
//			System.out.println("titleZh			="+titleZh			);

    		int		transfer		= 0;
 			v_int 		= (Integer)maps.get("transfer");
			if( v_int != null)
 				 transfer = v_int.intValue();
//			System.out.println("transfer		="+transfer			);

    		String	transFromM		=(String)maps.get("transFromM");
//			System.out.println("transFromM		="+transFromM		);
    		String	updated			=(String)maps.get("updated");
//			System.out.println("updated			="+updated			);
    		String	url				=(String)maps.get("url");
//			System.out.println("url				="+url				);

    		String	userTag			=(String)maps.get("userTag");
//			System.out.println("userTag			="+userTag			);
    		String	uuid			=(String)maps.get("uuid");

    		int		view			= 0;
 			v_int 		= (Integer)maps.get("view");
			if( v_int != null)
 				 view = v_int.intValue();
//			System.out.println("uuid			="+uuid				);

    		int		websiteId		= 0;
 			String s_web 		= (String)maps.get("websiteId");
			if( s_web != null && !s_web.isEmpty())
 				 websiteId = (new Integer(s_web)).intValue();
//			System.out.println("view			="+view				);

    		str=KafkaProperties.BEGIN
    				+abstractEn	  		+KafkaProperties.BETWEEN
    				+abstractZh	  		+KafkaProperties.BETWEEN
    				+author		  		+KafkaProperties.BETWEEN
    				+categoryId	      	+KafkaProperties.BETWEEN    
    				+comeFrom	 	 	+KafkaProperties.BETWEEN
    				+comeFromDb	  		+KafkaProperties.BETWEEN
    				+companies	  		+KafkaProperties.BETWEEN
    				+countryId		  	+KafkaProperties.BETWEEN    
    				+countryNameEn		+KafkaProperties.BETWEEN
    				+countryNameZh		+KafkaProperties.BETWEEN
    				+created	  		+KafkaProperties.BETWEEN
    				+delFlag		  	+KafkaProperties.BETWEEN    
    				+delReason	  		+KafkaProperties.BETWEEN
    				+districtId	      	+KafkaProperties.BETWEEN    
    				+districtNameEn 	+KafkaProperties.BETWEEN
    				+districtNameZh 	+KafkaProperties.BETWEEN
    				+docLength		  	+KafkaProperties.BETWEEN    
    				+domain		  		+KafkaProperties.BETWEEN
    				+domainId		  	+KafkaProperties.BETWEEN    
    				+eventCountryId   	+KafkaProperties.BETWEEN    
    				+eventDistrictId  	+KafkaProperties.BETWEEN    
    				+eventElement 		+KafkaProperties.BETWEEN
    				+eventId	  		+KafkaProperties.BETWEEN
    				+eventProvinceId  	+KafkaProperties.BETWEEN    
    				+isHome		  	  	+KafkaProperties.BETWEEN    
    				+isOriginal	  	  	+KafkaProperties.BETWEEN    
    				+isPicture		  	+KafkaProperties.BETWEEN    
    				+isSensitive	  	+KafkaProperties.BETWEEN    
    				+keywordsEn	  		+KafkaProperties.BETWEEN
    				+keywordsZh	  		+KafkaProperties.BETWEEN
    				+languageCode 		+KafkaProperties.BETWEEN
    				+languageTname		+KafkaProperties.BETWEEN
    				+latitude	  		+KafkaProperties.BETWEEN
    				+locationId	      	+KafkaProperties.BETWEEN    
    				+bigintitude  		+KafkaProperties.BETWEEN
    				+mediaLevel	      	+KafkaProperties.BETWEEN    
    				+mediaNameEn  		+KafkaProperties.BETWEEN
    				+mediaNameSrc 		+KafkaProperties.BETWEEN
    				+mediaNameZh  		+KafkaProperties.BETWEEN
    				+mediaTname	  		+KafkaProperties.BETWEEN
    				+mediaType		  	+KafkaProperties.BETWEEN    
    				+products	  		+KafkaProperties.BETWEEN
    				+provinceId	      	+KafkaProperties.BETWEEN    
    				+provinceNameEn 	+KafkaProperties.BETWEEN
    				+provinceNameZh 	+KafkaProperties.BETWEEN
    				+pubAffairKeyword	+KafkaProperties.BETWEEN
    				+pubAffairType	  	+KafkaProperties.BETWEEN    
    				+pubdate	  		+KafkaProperties.BETWEEN
    				+pubTime		  	+KafkaProperties.BETWEEN    
    				+pv			      	+KafkaProperties.BETWEEN    
    				+sensitiveCls 		+KafkaProperties.BETWEEN
    				+sensitiveType	  	+KafkaProperties.BETWEEN    
    				+sentimentId	  	+KafkaProperties.BETWEEN    
    				+similarityId 		+KafkaProperties.BETWEEN
    				+textEn		  		+KafkaProperties.BETWEEN
    				+textQuality	  	+KafkaProperties.BETWEEN    
    				+textSrc	  		+KafkaProperties.BETWEEN
    				+textZh		  		+KafkaProperties.BETWEEN
    				+titleEn	  		+KafkaProperties.BETWEEN
    				+titleSrc	  		+KafkaProperties.BETWEEN
    				+titleZh	  		+KafkaProperties.BETWEEN
    				+transfer		  	+KafkaProperties.BETWEEN    
    				+transFromM	  		+KafkaProperties.BETWEEN
    				+updated	  		+KafkaProperties.BETWEEN
    				+url		  		+KafkaProperties.BETWEEN
    				+userTag	  		+KafkaProperties.BETWEEN
    				+uuid		  		+KafkaProperties.BETWEEN
    				+view               +KafkaProperties.BETWEEN
    				+websiteId;
    	}else{
    		System.out.println("MsgType=["+mt+"] not support now");
    	}
//    	System.out.println(str);
    	return str;
    }
	
	
	public void doHDFS(String msg,Date date,String mt){
		Map map = parseJson(msg);//将json数据放入map中
		String str="";
		if(null!=map){
			str=getStr(map,mt);//将map数据转化成String
		}
		SimpleDateFormat dateFormat=new SimpleDateFormat("yyyyMMdd/HH");
		SimpleDateFormat timeFormat=new SimpleDateFormat("HHmm");
		String dateStr=dateFormat.format(date)+"/";
		String timeStr=timeFormat.format(date);
		if(!"".equals(str)){
			String filename="";
			if(mt.equals("news")){
				filename=KafkaProperties.NEWDIR+dateStr+timeStr+".txt";
				if(filename.equals(newsFileName)){//判断是否需要生成新的文件
					newsContinueWrite(str);
				}else{
					newsWriteHDFS(filename, str);
					newsFileName=filename;
				}
			}else if(mt.equals("soc")){
				filename=KafkaProperties.SOCDIR+dateStr+timeStr+".txt";
				if(filename.equals(socFileName)){
					socContinueWrite(str);
				}else{
					socWriteHDFS(filename, str);
					socFileName=filename;
				}
			}
		}
	}
}
