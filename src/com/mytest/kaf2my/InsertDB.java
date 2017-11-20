package com.mytest.kaf2my;

//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.io.InputStream;
//import java.sql.Connection;
//import java.sql.Statement;
// 
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Properties;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;

public class InsertDB {
	
//    private static Connection m_conn = null;
//	
//    public static Connection getConn(String url) throws Exception {    
//        Class.forName("com.mysql.jdbc.Driver");
//        return DriverManager.getConnection(url);
//    }
//    
//    private  Map parseJson (String jsonStr) {  
//    	
//   	    Map maps = (Map)JSON.parse(jsonStr);  
//   	    /*for (Object map : maps.entrySet()){  
//    		System.out.println(((Map.Entry)map).getKey()+"=[" + ((Map.Entry)map).getValue()+"]");  
//    	}*/
//    	return maps;
//    }    
//   
//    public String getFilterStr(String str){//替换字符串中的\和'
//    	
//		if( str != null ){
//			str = str.replace("\"", "\\\"");
//			str = str.replace("'", "");
//		}
//		return str;
//    }
//    
//    private  String getTabSql (Map maps,String mt) {    
//    	
//    	String sql="";
//    	
//    	//String mid = (String)maps.get("myId");
//    	//System.out.println("mid="+mid);
//    	//String uuid = (String)maps.get("uuid");
//    	//System.out.println("uuid="+uuid);  
//    	
//    	if(mt.equals("soc")){//社交消息
//    		//if (mid != null && !mid.isEmpty()){
//    		
//			Integer  v_int;
//			Long v_long;
//			
//			int			atdCnt			= 0;
// 			v_int 		= (Integer)maps.get("atdCnt");
//			if( v_int != null)
// 				 atdCnt = v_int.intValue();
//			System.out.println("atdCnt="+atdCnt);    
//
//
// 			String		city			=(String)maps.get("city");
//			System.out.println("city="+city);
// 			
// 			int			cityId			= 0;
// 			v_int 		= (Integer)maps.get("cityId");
//			if( v_int != null)
// 				 cityId = v_int.intValue();
//			System.out.println("cityId="+cityId);   	
//
//
// 			int			cmtCnt			= 0;
// 			v_int 		= (Integer)maps.get("cmtCnt");
//			if( v_int != null)
// 				 cmtCnt = v_int.intValue();
//			System.out.println("cmtCnt="+cmtCnt);    
//
//
// 			long		commentSince	= 0;
// 			v_int 		= (Integer)maps.get("commentSince");
//			if( v_int != null)
// 				 commentSince = v_int.intValue();
//			System.out.println("commentSince="+commentSince);    
//
// 			
// 			String		companies		=(String)maps.get("companies");
//			System.out.println("companies="+companies);
//			
// 			String		country			=(String)maps.get("country");
//			System.out.println("country="+country);
//
// 			int			delFlag			= 0;
// 			v_int 		= (Integer)maps.get("delFlag");
//			if( v_int != null)
// 				delFlag  = v_int.intValue();
//			System.out.println("delFlag="+delFlag);    
//
//
// 			String		delReason		=(String)maps.get("delReason");
//			System.out.println("delReason="+delReason);
//
// 			long		eventCountryId	= 0;
// 			v_int 		= (Integer)maps.get("eventCountryId");
//			if( v_int != null)
// 				 eventCountryId = v_int.intValue();
//			System.out.println("eventCountryId="+eventCountryId);    
//
//
// 			long		eventDistrictId	= 0;
// 			v_int 		= (Integer)maps.get("eventDistrictId");
//			if( v_int != null)
// 				 eventDistrictId = v_int.intValue();
//			System.out.println("eventDistrictId="+eventDistrictId);    
//
// 			String		eventElement	=(String)maps.get("eventElement");
//			System.out.println("eventElement="+eventElement);
// 			String		eventId			=(String)maps.get("eventId");
//			System.out.println("eventId="+eventId);
//
// 			long		eventProvinceId	= 0;
// 			v_int 		= (Integer)maps.get("eventProvinceId");
//			if( v_int != null)
// 				 eventProvinceId = v_int.intValue();
//			System.out.println("eventProvinceId="+eventProvinceId);    
//
// 			int			flwCnt			= 0;
// 			v_int 		= (Integer)maps.get("flwCnt");
//			if( v_int != null)
// 				 flwCnt = v_int.intValue();
//			System.out.println("flwCnt="+flwCnt);    
//
// 			int			frdCnt			= 0;
// 			v_int 		= (Integer)maps.get("frdCnt");
//			if( v_int != null)
// 				 frdCnt = v_int.intValue();
//			System.out.println("frdCnt="+frdCnt);   	
//
// 			int			gender			= 0;
// 			v_int 		= (Integer)maps.get("gender");
//			if( v_int != null)
// 				 gender = v_int.intValue();
//			System.out.println("gender="+gender);   	
//
//
// 			String		id				=(String)maps.get("id");
//			System.out.println("id="+id);
//
// 			int			isOri			= 0;
// 			v_int 		= (Integer)maps.get("isOri");
//			if( v_int != null)
// 				 isOri = v_int.intValue();
//			System.out.println("isOri="+isOri);   	
//
//
// 			String		languageCode	=(String)maps.get("languageCode");
//			System.out.println("languageCode="+languageCode);
// 			String		latitude		=(String)maps.get("latitude");
//			System.out.println("latitude="+latitude);
//
// 			long		locationId		= 0;
// 			v_int 		= (Integer)maps.get("locationId");
//			if( v_int != null)
// 				 locationId = v_int.intValue();
//			System.out.println("locationId="+locationId);   	
//
//
// 			String		bigintitude		=(String)maps.get("bigintitude");
//			System.out.println("bigintitude="+bigintitude);
// 			String		myId			=(String)maps.get("myId");
//			System.out.println("myId="+myId);
// 			String		name			=(String)maps.get("name");
//			System.out.println("name="+name);
// 			String		products		=(String)maps.get("products");
//			System.out.println("products="+products);
// 			String		province		=(String)maps.get("province");
//			System.out.println("province="+province);
//
// 			int			provinceId		= 0;
// 			v_int 		= (Integer)maps.get("provinceId");
//			if( v_int != null)
// 				provinceId  = v_int.intValue();
//			System.out.println("provinceId="+provinceId);    
//
//
// 			String		pubAffairKeyword=(String)maps.get("pubAffairKeyword");
//			System.out.println("pubAffairKeyword="+pubAffairKeyword);
//
// 			int			pubAffairType	= 0;
// 			v_int 		= (Integer)maps.get("pubAffairType");
//			if( v_int != null)
// 				 pubAffairType = v_int.intValue();
//			System.out.println("pubAffairType="+pubAffairType);    
//
//
// 			long		repostSince		= 0;
// 			v_int 		= (Integer)maps.get("repostSince");
//			if( v_int != null)
// 				 repostSince = v_int.longValue();
//			System.out.println("repostSince="+repostSince);    
//
//
// 			int			rpsCnt			= 0;
// 			v_int 		= (Integer)maps.get("rpsCnt");
//			if( v_int != null)
// 				 rpsCnt = v_int.intValue();
//			System.out.println("rpsCnt="+rpsCnt);    
// 			
//			/*
// 			int			sentiment		= 0; 
// 			String v_str= (String)maps.get("sentiment");
// 			if(v_str != null && !v_str.isEmpty())
// 				sentiment = (new Integer(v_str)).intValue();
// 			*/	
// 			
// 			String		sentiment		=(String)maps.get("sentiment");
//			System.out.println("sentiment_XXXXX="+sentiment);
// 			
// 			int			sentimentOrient	= 0;
// 			v_int= (Integer)maps.get("sentimentOrient");
// 			if(v_int != null)
// 				sentimentOrient = v_int.intValue();
//			System.out.println("sentimentOrient="+sentimentOrient);    
//
// 			String		sourceType		=(String)maps.get("sourceType");
//			System.out.println("sourceType="+sourceType);
// 			String		sourceWeiboId	=(String)maps.get("sourceWeiboId");
//			System.out.println("sourceWeiboId="+sourceWeiboId);
// 			String		sourceWeiboMyId	=(String)maps.get("sourceWeiboMyId");
//			System.out.println("sourceWeiboMyId="+sourceWeiboMyId);
//
// 			int			staCnt			= 0;
// 			v_int= (Integer)maps.get("staCnt");
// 			if(v_int != null)
// 				staCnt = v_int.intValue();
//			System.out.println("staCnt="+staCnt);    
//
// 			String		text			=(String)maps.get("text");
// 			if(text != null )	text = getFilterStr(text);
//			System.out.println("text="+text);
//
// 			String		textEn			=(String)maps.get("textEn");
// 			if(textEn != null )	textEn = getFilterStr(textEn);
//			System.out.println("textEn="+textEn);
//
// 			int			textLen			= 0;
// 			v_int= (Integer)maps.get("textLen");
// 			if(v_int != null)
// 				textLen = v_int.intValue();
//			System.out.println("textLen="+textLen);  	
//
// 			String		textZh			=(String)maps.get("textZh");
//			System.out.println("textZh="+textZh);
//
// 			long		time			= 0;
// 			v_long= (Long)maps.get("time");
// 			if(v_int != null)
// 				time  = v_long.longValue();
//
//			System.out.println("time="+time);    
//
// 			String		timeStr			=(String)maps.get("timeStr");
//			System.out.println("timeStr="+timeStr);
// 			String		title			=(String)maps.get("title");
// 			if(title != null )	title = getFilterStr(title);
//			System.out.println("title="+title);
//
// 			long		updateTime		= 0;
// 			v_long= (Long)maps.get("updateTime");
// 			if(v_long != null)
// 				updateTime = v_long.longValue();
//			System.out.println("updateTime="+updateTime);   	
//
//
// 			String		updateTimeStr	=(String)maps.get("updateTimeStr");
//			System.out.println("updateTimeStr="+updateTimeStr);
// 			String		url				=(String)maps.get("url");
//			System.out.println("url="+url);
// 			String		userAvatar		=(String)maps.get("userAvatar");
//			System.out.println("userAvatar="+userAvatar);
// 			String		userId			=(String)maps.get("userId");
//			System.out.println("userId="+userId);
// 			String		userTag			=(String)maps.get("userTag");
//			System.out.println("userTag="+userTag);
// 			String		userType		=(String)maps.get("userType");
//			System.out.println("userType="+userType);
//
// 			int			verified		= 0;
// 			v_int= (Integer)maps.get("verified");
// 			if(v_int != null)
// 				verified = v_int.intValue();
//			System.out.println("verified="+verified);    
// 			
// 			String		verifiedReason	=(String)maps.get("verifiedReason");
//			System.out.println("verifiedReason="+verifiedReason);
//
// 			long		view			= 0;
// 			v_int= (Integer)maps.get("view");
// 			if(v_int != null)
// 				view = v_int.intValue();
//			System.out.println("view="+view);    
//
//    		sql="insert into zyyt_soc( atdCnt, city, cityId, cmtCnt, commentSince, companies,country, delFlag, delReason, eventCountryId, eventDistrictId, eventElement,	eventId, eventProvinceId, flwCnt, frdCnt, gender, id, isOri,"
//    				+"languageCode, latitude, locationId, bigintitude, myId, name, products, province, provinceId, pubAffairKeyword, pubAffairType, repostSince, rpsCnt, sentiment, sentimentOrient, sourceType, sourceWeiboId, sourceWeiboMyId, staCnt, text,"
//    				+"textEn, textLen, textZh, time, timeStr, title, updateTime, updateTimeStr, url	, userAvatar, userId, userTag, userType, verified, verifiedReason, view	)values("
//
//    				+ atdCnt				+","    
//    				+ "'" +city				+"'"+","
//    				+ cityId				+","   	
//    				+ cmtCnt				+","    
//    				+ commentSince			+","    
//    				+ "'" +companies		+"'"+","
//    				+ "'" +country			+"'"+","
//    				+ delFlag				+","    
//    				+ "'" +delReason		+"'"+","
//    				+ eventCountryId		+","    
//    				+ eventDistrictId		+","    
//    				+ "'" +eventElement		+"'"+","
//    				+ "'" +eventId			+"'"+","
//    				+ eventProvinceId		+","    
//    				+ flwCnt				+","    
//    				+ frdCnt				+","   	
//    				+ gender				+","   	
//    				+ "'" +id				+"'"+","
//    				+ isOri					+","   	
//    				+ "'" +languageCode		+"'"+","
//    				+ "'" +latitude			+"'"+","
//    				+ locationId			+","   	
//    				+ "'" +bigintitude		+"'"+","
//    				+ "'" +myId				+"'"+","
//    				+ "'" +name				+"'"+","
//    				+ "'" +products			+"'"+","
//    				+ "'" +province			+"'"+","
//    				+ provinceId			+","    
//    				+ "'" +pubAffairKeyword	+"'"+","
//    				+ pubAffairType			+","    
//    				+ repostSince			+","    
//    				+ rpsCnt				+","    
//    				+ "'" +sentiment		+"'"+","    
//    				+ sentimentOrient		+","    
//    				+ "'" +sourceType		+"'"+","
//    				+ "'" +sourceWeiboId	+"'"+","
//    				+ "'" +sourceWeiboMyId	+"'"+","
//    				+ staCnt				+","    
//    				+ "'" +text				+"'"+","
//    				+ "'" +textEn			+"'"+","
//    				+ textLen				+","  	
//    				+ "'" +textZh			+"'"+","
//    				+ time					+","    
//    				+ "'" +timeStr			+"'"+","
//    				+ "'" +title			+"'"+","
//    				+ updateTime			+","   	
//    				+ "'" +updateTimeStr	+"'"+","
//    				+ "'" +url				+"'"+","
//    				+ "'" +userAvatar		+"'"+","
//    				+ "'" +userId			+"'"+","
//    				+ "'" +userTag			+"'"+","
//    				+ "'" +userType			+"'"+","
//    				+ verified				+","    
//    				+ "'" +verifiedReason	+"'"+","
//    				+ view    						    				
//    			    +")";
//    		
//    	} else if( mt.equals("news") ){//新闻消息
//    		
//    	//if( uuid!=null && !uuid.isEmpty() ){
//  
//			Integer  v_int;
//			Long v_long;
//			JSONArray	v_ja;
//
//			String 	abstractEn		=(String)maps.get("abstractEn");    
//			if(abstractEn != null )	abstractEn = getFilterStr(abstractEn);
//			System.out.println("abstractEn		="+abstractEn		);
//			
//    		String 	abstractZh		=(String)maps.get("abstractZh");
//    		if(abstractZh != null )	abstractZh = getFilterStr(abstractZh);
//			System.out.println("abstractZh		="+abstractZh		);
//			
//    		String 	author			=(String)maps.get("author");
//			System.out.println("author			="+author			);
//
//    		int 	categoryId		= 0;
// 			v_int 		= (Integer)maps.get("categoryId");
//			if( v_int != null)
// 				categoryId  = v_int.intValue();
//			System.out.println("categoryId		="+categoryId		);
//
//
//    		String	comeFrom		=(String)maps.get("comeFrom");
//			System.out.println("comeFrom		="+comeFrom			);
//			
//    		String	comeFromDb		=(String)maps.get("comeFromDb");
//			System.out.println("comeFromDb		="+comeFromDb		);
//			
//			v_ja		=(JSONArray)maps.get("companies");
//			String companies = v_ja.toString();
//			System.out.println("companies		="+companies		);
//			
//    		long	countryId		= 0;
// 			v_long 		= (Long)maps.get("countryId");
//			if( v_long != null)
// 				 countryId = v_long.longValue();
//			System.out.println("countryId		="+countryId		);
//
//    		String	countryNameEn	=(String)maps.get("countryNameEn");
//			System.out.println("countryNameEn	="+countryNameEn	);
//    		String	countryNameZh	=(String)maps.get("countryNameZh");
//			System.out.println("countryNameZh	="+countryNameZh	);
//    		String	created			=(String)maps.get("created");
//			System.out.println("created			="+created			);
//
//    		int		delFlag			= 0;
// 			v_int 		= (Integer)maps.get("delFlag");
//			if( v_int != null)
// 				 delFlag = v_int.intValue();
//			System.out.println("delFlag			="+delFlag			);
//
//
//    		String	delReason		=(String)maps.get("delReason");
//			System.out.println("delReason		="+delReason		);
//
//    		long	districtId		= 0;
// 			v_long 		= (Long)maps.get("districtId");
//			if( v_long != null)
// 				 districtId = v_int.longValue();
//			System.out.println("districtId		="+districtId		);
//
//
//    		String	districtNameEn	=(String)maps.get("districtNameEn");
//			System.out.println("districtNameEn	="+districtNameEn	);
//    		String	districtNameZh	=(String)maps.get("districtNameZh");
//			System.out.println("districtNameZh	="+districtNameZh	);
//
//    		int		docLength		= 0;
// 			v_int 		= (Integer)maps.get("docLength");
//			if( v_int != null)
// 				docLength  = v_int.intValue();
//			System.out.println("docLength		="+docLength		);
//
//
//    		String	domain			=(String)maps.get("domain");
//			System.out.println("domain			="+domain			);
//
//    		int		domainId		= 0;
// 			v_int 		= (Integer)maps.get("domainId");
//			if( v_int != null)
// 				 domainId = v_int.intValue();
//			System.out.println("domainId		="+domainId			);
//
//
//    		int	eventCountryId	= 0;      		//***int not long 
// 			v_int 		= (Integer)maps.get("eventCountryId");
//			if( v_int != null)
// 				eventCountryId  = v_int.intValue();
//			System.out.println("eventCountryId	="+eventCountryId	);
//
//    		int	eventDistrictId	= 0;    		//***int not long 
// 			v_int 		= (Integer)maps.get("eventDistrictId");
//			if( v_int != null)
// 				 eventDistrictId = v_int.intValue();
//			System.out.println("eventDistrictId	="+eventDistrictId	);
//
//
//    		String	eventElement	=(String)maps.get("eventElement");
//			System.out.println("eventElement	="+eventElement		);
//    		String	eventId			=(String)maps.get("eventId");
//			System.out.println("eventId			="+eventId			);
//
//    		int		eventProvinceId	= 0;
// 			v_int 		= (Integer)maps.get("eventProvinceId");
//			if( v_int != null)
// 				 eventProvinceId = v_int.intValue();
//			System.out.println("eventProvinceId	="+eventProvinceId	);
//
//    		int		isHome			= 0;
// 			String s_isHome 		= (String)maps.get("isHome");
//			if( s_isHome != null)
// 				 isHome = (new Integer(s_isHome)).intValue();
//			System.out.println("isHome			="+isHome			);
//
//    		int		isOriginal		= 0;
// 			String s_iso 		= (String)maps.get("isOriginal");
//			if( s_iso != null)
// 				 isOriginal = (new Integer(s_iso)).intValue();
//			System.out.println("isOriginal		="+isOriginal		);
//
//    		int		isPicture		= 0;
// 			String s_isp 		= (String)maps.get("isPicture");
//			if( s_isp != null)
// 				 isPicture = (new Integer(s_isp)).intValue();
//			System.out.println("isPicture		="+isPicture		);
//
//
//    		int		isSensitive		= 0;
// 			v_int 		= (Integer)maps.get("isSensitive");
//			if( v_int != null)
// 				 isSensitive = v_int.intValue();
//			System.out.println("isSensitive		="+isSensitive		);
//			
//			v_ja		=(JSONArray)maps.get("keywordsEn");
//			String keywordsEn = v_ja.toString();
//			System.out.println("keywordsEn		="+keywordsEn		);
//			
//			
//			v_ja		=(JSONArray)maps.get("keywordsZh");
//    		String	keywordsZh		=v_ja.toString();
//			System.out.println("keywordsZh		="+keywordsZh		);
//			
//    		String	languageCode	=(String)maps.get("languageCode");
//			System.out.println("languageCode	="+languageCode		);
//    		String	languageTname	=(String)maps.get("languageTname");
//			System.out.println("languageTname	="+languageTname	);
//    		String	latitude		=(String)maps.get("latitude");
//			System.out.println("latitude		="+latitude			);
//
//    		int		locationId		= 0;
// 			v_int 		= (Integer)maps.get("locationId");
//			if( v_int != null)
// 				 locationId = v_int.intValue();
//			System.out.println("locationId		="+locationId		);
//
//    		String	bigintitude		=(String)maps.get("bigintitude");
//			System.out.println("bigintitude		="+bigintitude		);
//
//    		int		mediaLevel		= 0;
// 			v_int 		= (Integer)maps.get("mediaLevel");
//			if( v_int != null)
// 				 mediaLevel = v_int.intValue();
//			System.out.println("mediaLevel		="+mediaLevel		);
//
//    		String	mediaNameEn		=(String)maps.get("mediaNameEn");
//    		if(mediaNameEn != null )	mediaNameEn = getFilterStr(mediaNameEn);    		
//			System.out.println("mediaNameEn		="+mediaNameEn		);
//			
//    		String	mediaNameSrc	=(String)maps.get("mediaNameSrc");
//			System.out.println("mediaNameSrc	="+mediaNameSrc		);
//			
//    		String	mediaNameZh		=(String)maps.get("mediaNameZh");
//			System.out.println("mediaNameZh		="+mediaNameZh		);
//
//
//    		String	mediaTname		=(String)maps.get("mediaTname");
//    		
//			System.out.println("mediaTname		="+mediaTname		);
//
//    		int		mediaType		= 0;
// 			v_int 		= (Integer)maps.get("mediaType");
//			if( v_int != null)
// 				 mediaType = v_int.intValue();
//			System.out.println("mediaType		="+mediaType		);
//			
//			v_ja		=(JSONArray)maps.get("products");
//    		String	products		=v_ja.toString();
//			System.out.println("products		="+products			);
//
//    		long	provinceId		= 0;
//    		v_long 		= (Long)maps.get("provinceId");
//			if( v_long != null)
//				provinceId = v_long.longValue();
//			System.out.println("provinceId		="+provinceId		);
//
//    		String	provinceNameEn	=(String)maps.get("provinceNameEn");
//			System.out.println("provinceNameEn	="+provinceNameEn	);
//    		String	provinceNameZh	=(String)maps.get("provinceNameZh");
//			System.out.println("provinceNameZh	="+provinceNameZh	);
//    		String	pubAffairKeyword=(String)maps.get("pubAffairKeyword");
//			System.out.println("pubAffairKeyword="+pubAffairKeyword	);
//
//    		int		pubAffairType	= 0;
// 			v_int 		= (Integer)maps.get("pubAffairType");
//			if( v_int != null)
// 				 pubAffairType = v_int.intValue();
//			System.out.println("pubAffairType	="+pubAffairType	);
//
//    		String	pubdate			=(String)maps.get("pubdate");
//			System.out.println("pubdate			="+pubdate			);
//
//    		long	pubTime			= 0;
//    		v_long 		= (Long)maps.get("pubTime");
//			if( v_long != null)
// 				pubTime  = v_long.longValue();
//			System.out.println("pubTime			="+pubTime			);
//
//    		int		pv				=((Integer)maps.get("pv")).intValue();    
// 			v_int 		= (Integer)maps.get("pv");
//			if( v_int != null)
// 				pv  = v_int.intValue();
//			System.out.println("pv				="+pv				);
//
//    		String	sensitiveCls	=(String)maps.get("sensitiveCls");
//			System.out.println("sensitiveCls	="+sensitiveCls		);
//
//    		int		sensitiveType	= 0;
// 			v_int 		= (Integer)maps.get("sensitiveType");
//			if( v_int != null)
// 				 sensitiveType = v_int.intValue();
//			System.out.println("sensitiveType	="+sensitiveType	);
//
//    		int		sentimentId		= 0;
// 			v_int 		= (Integer)maps.get("sentimentId");
//			if( v_int != null)
// 				 sentimentId = v_int.intValue();
//			System.out.println("sentimentId		="+sentimentId		);
//
//    		String	similarityId	=(String)maps.get("similarityId");
//			System.out.println("similarityId	="+similarityId		);
//    		String	textEn			=(String)maps.get("textEn");
//			System.out.println("textEn			="+textEn			);
//
//    		long	textQuality		= 0;
//    		v_long 		= (Long)maps.get("textQuality");
//			if( v_long != null)
// 				 textQuality = v_long.longValue();
//			System.out.println("textQuality		="+textQuality		);
//
//    		String	textSrc			=(String)maps.get("textSrc");
//    		if(textSrc != null )	textSrc = getFilterStr(textSrc);
//    		System.out.println("textSrc			="+textSrc			);
//    		
//    		String	textZh			=(String)maps.get("textZh");
//    		if(textZh != null )	textZh = getFilterStr(textZh);
//			System.out.println("textZh			="+textZh			);
//			
//    		String	titleEn			=(String)maps.get("titleEn");
//    		if(titleEn != null )	titleEn = getFilterStr(titleEn);   		
//			System.out.println("titleEn			="+titleEn			);
//			
//    		String	titleSrc		=(String)maps.get("titleSrc");
//    		if(titleSrc != null )	titleSrc = getFilterStr(titleSrc);    		
//			System.out.println("titleSrc		="+titleSrc			);
//			
//    		String	titleZh			=(String)maps.get("titleZh");
//    		if(titleZh != null )	titleZh = getFilterStr(titleZh);
//			System.out.println("titleZh			="+titleZh			);
//
//    		int		transfer		= 0;
// 			v_int 		= (Integer)maps.get("transfer");
//			if( v_int != null)
// 				 transfer = v_int.intValue();
//			System.out.println("transfer		="+transfer			);
//
//    		String	transFromM		=(String)maps.get("transFromM");
//			System.out.println("transFromM		="+transFromM		);
//    		String	updated			=(String)maps.get("updated");
//			System.out.println("updated			="+updated			);
//    		String	url				=(String)maps.get("url");
//			System.out.println("url				="+url				);
//
//    		String	userTag			=(String)maps.get("userTag");
//			System.out.println("userTag			="+userTag			);
//    		String	uuid			=(String)maps.get("uuid");
//
//    		int		view			= 0;
// 			v_int 		= (Integer)maps.get("view");
//			if( v_int != null)
// 				 view = v_int.intValue();
//			System.out.println("uuid			="+uuid				);
//
//    		int		websiteId		= 0;
// 			String s_web 		= (String)maps.get("websiteId");
//			if( s_web != null && !s_web.isEmpty())
// 				 websiteId = (new Integer(s_web)).intValue();
//			System.out.println("view			="+view				);
//
//    		sql="insert into zyyt_news(abstractEn,abstractZh,author,categoryId,comeFrom,comeFromDb,companies,countryId,countryNameEn,countryNameZh,created,delFlag,delReason,districtId,districtNameEn,districtNameZh,docLength,domain,domainId,eventCountryId,"
//    				+ "eventDistrictId,eventElement,eventId,eventProvinceId,isHome,isOriginal,isPicture,isSensitive,keywordsEn,keywordsZh,languageCode,languageTname,latitude,locationId,bigintitude,mediaLevel,mediaNameEn,mediaNameSrc,mediaNameZh,mediaTname,"
//    				+ "mediaType,products,provinceId,provinceNameEn,provinceNameZh,pubAffairKeyword,pubAffairType,pubdate,pubTime,pv,sensitiveCls,sensitiveType,sentimentId,similarityId,textEn,textQuality,textSrc,textZh,titleEn,titleSrc,titleZh,transfer,"
//    				+ "transFromM,updated,url,userTag,uuid,view,websiteId) values("
//    				
//    				+"'"+abstractEn	  +"'"+","
//    				+"'"+abstractZh	  +"'"+","
//    				+"'"+author		  +"'"+","
//    				+categoryId	      +","    
//    				+"'"+comeFrom	  +"'"+","
//    				+"'"+comeFromDb	  +"'"+","
//    				+"'"+companies	  +"'"+","
//    				+countryId		  +","    
//    				+"'"+countryNameEn+"'"+","
//    				+"'"+countryNameZh+"'"+","
//    				+"'"+created	  +"'"+","
//    				+delFlag		  +","    
//    				+"'"+delReason	  +"'"+","
//    				+districtId	      +","    
//    				+"'"+districtNameEn +"'"+","
//    				+"'"+districtNameZh +"'"+","
//    				+docLength		  +","    
//    				+"'"+domain		  +"'"+","
//    				+domainId		  +","    
//    				+eventCountryId   +","    
//    				+eventDistrictId  +","    
//    				+"'"+eventElement +"'"+","
//    				+"'"+eventId	  +"'"+","
//    				+eventProvinceId  +","    
//    				+isHome		  	  +","    
//    				+isOriginal	  	  +","    
//    				+isPicture		  +","    
//    				+isSensitive	  +","    
//    				+"'"+keywordsEn	  +"'"+","
//    				+"'"+keywordsZh	  +"'"+","
//    				+"'"+languageCode +"'"+","
//    				+"'"+languageTname+"'"+","
//    				+"'"+latitude	  +"'"+","
//    				+locationId	      +","    
//    				+"'"+bigintitude  +"'"+","
//    				+mediaLevel	      +","    
//    				+"'"+mediaNameEn  +"'"+","
//    				+"'"+mediaNameSrc +"'"+","
//    				+"'"+mediaNameZh  +"'"+","
//    				+"'"+mediaTname	  +"'"+","
//    				+mediaType		  +","    
//    				+"'"+products	  +"'"+","
//    				+provinceId	      +","    
//    				+"'"+provinceNameEn +"'"+","
//    				+"'"+provinceNameZh +"'"+","
//    				+"'"+pubAffairKeyword+"'"+","
//    				+pubAffairType	  +","    
//    				+"'"+pubdate	  +"'"+","
//    				+pubTime		  +","    
//    				+pv			      +","    
//    				+"'"+sensitiveCls +"'"+","
//    				+sensitiveType	  +","    
//    				+sentimentId	  +","    
//    				+"'"+similarityId +"'"+","
//    				+"'"+textEn		  +"'"+","
//    				+textQuality	  +","    
//    				+"'"+textSrc	  +"'"+","
//    				+"'"+textZh		  +"'"+","
//    				+"'"+titleEn	  +"'"+","
//    				+"'"+titleSrc	  +"'"+","
//    				+"'"+titleZh	  +"'"+","
//    				+transfer		  +","    
//    				+"'"+transFromM	  +"'"+","
//    				+"'"+updated	  +"'"+","
//    				+"'"+url		  +"'"+","
//    				+"'"+userTag	  +"'"+","
//    				+"'"+uuid		  +"'"+","
//    				+view			  +","    
//    				+websiteId  
//    				
//                    +")" ;
//    	}else{
//    		System.out.println("MsgType=["+mt+"] not support now");
//    	}
//    	
//    	return sql;
//    }
//
///*    
//    public static void main(String[] arg)    {
//    	
//    	
//        //String str = "{\"a\":\"zhangsan\",\"b\":\"lisi\",\"c\":\"wangwu\",\"d\":\"maliu\"}";
//    	String soc_str="{\"atdCnt\": 0,  \"city\": \"中国\",  \"cityId\": -1,  \"cmtCnt\": 0,  \"commentSince\": 12345,  \"companies\": \"\", \"country\": \"中国\",  \"delFlag\": 0,  \"eventCountryId\": 0,  \"eventDistrictId\": 0,  \"eventEntity\": \"json=[]\",  \"eventProvinceId\": 0,  \"flwCnt\": 33,  \"frdCnt\": 0,  \"gender\": -1,  \"id\": \"08149103510549667AE537FE566114D\",  \"isOri\": 1,  \"languageCode\": \"zh\",  \"latitude\": \"\",  \"locationId\": 0,  \"longitude\": \"\",  \"myId\": \"08149103510549667AE537FE566114D\",  \"name\": \"June_小胖不胖我不胖\",  \"nameEntity\": {    \"MainDescribes\": {   \"landmark\": {  \"geo_lat\": \"\",  \"geo_long\": \"\"  },  \"location\": \"\",      \"organization\": \"\",      \"person\": \"周雨\"    },    \"contain_location\": [],    \"contain_organization\": [],    \"contain_person\": [], \"location_landmark\": []  },  \"person\": [    \"5ZGo6Zuo\"  ],  \"products\": \"\",  \"province\": \"中国\",  \"provinceId\": -1,  \"repostSince\": 54321,  \"rpsCnt\": 0,  \"sentiment\": \"\",  \"sentimentOrient\": -1,  \"sourceType\": \"weibo\",  \"sourceWeibold\": \"\",  \"staCnt\": 0,  \"tagId\": 35,  \"text\": \"小豹子变成了小兔子，超可爱//@从惊蛰一路走到霜降_四呆:啵啵你！#周雨为什么这么可爱# 查看图片\",  \"textEn\": \"\",  \"textLen\": 49,  \"textZh\": \"小豹子变成了小兔子，超可爱//@从惊蛰一路走到霜降_四呆:啵啵你！#周雨为什么这么可爱# 查看图片\",  \"time\": 1491034855000,  \"timeStr\": \"2017-04-01 16:20:55\",  \"title\": \"小豹子变成了小兔子，超可爱//@从惊蛰一路走到霜降_四呆:啵啵你！#周雨为什么这么可爱# 查看图片\",  \"updateTime\": 1491035545506,  \"updateTimeStr\": \"2017-04-01 16:32:25\",  \"url\": \"http://weibo.com/5770141925/ECv4S1bu3\",  \"userAvatar\": \"http://tva1.sinaimg.cn/crop.97.63.388.388.180/006iuVvfjw8fabmkcbe9ij30jg0gldhd.jpg\",  \"userId\": \"\",  \"userType\": \"其他\",  \"verified\": 1,  \"verifiedReason\": \"\",  \"view\": 0}";
//    	String new_str="{\"abstractEn\": \"abstract en\",  \"abstractZh\": \"abstract zh\",  \"categoryId\": 0,  \"comeFrom\": \"Hongmai\",  \"companies\": [],  \"countryNameEn\": \"India\",  \"countryNameZh\": \"印度\",  \"created\": \"2017-04-02 10:05:32\",  \"delFlag\": 0,  \"districtNameEn\": \"Ahmedabad\",  \"districtNameZh\": \"阿穆达巴\",  \"docLength\": 2640,  \"docQuality\": 0,  \"domain\": \"engineeringnews.co.za\",  \"domainId\": 36342,  \"eventCountryId\": 259,  \"eventDistrictId\": 0,  \"eventEntity\": \"json=\",  \"eventProvinceId\": 0,  \"isHome\": \"0\",  \"isOriginal\": \"0\",  \"isPicture\": \"0\",  \"isSensitive\": 0,  \"keywordsEn\": [    \"South African\",    \"published material\",    \"project-tracking service\",    \"user\",    \"intelligence\"  ],  \"keywordsZh\": [],  \"languageCode\": \"en\",  \"languageTname\": \"英语\",  \"latitude\": \"\",  \"locationId\": 0,  \"longitude\": \"\",  \"mediaLevel\": 3,  \"mediaNameEn\": \"\",  \"mediaNameSrc\": \"Engineering News\",  \"mediaNameZh\": \"Engineering News\",  \"mediaTname\": \"新闻\",  \"mediaType\": 1,  \"namespace\": {    \"MainDescribes\": {      \"landmark\": {        \"country\": \"瑞典\",        \"countryId\": \"259\",        \"geo_lat\": \"\",        \"geo_long\": \"\"      },      \"location\": \"Sweden\",      \"organization\": \"Iron-Ore Mining A project-tracking\",      \"person\": \"Per\"    },    \"contain_location\": [],    \"contain_organization\": [],    \"contain_person\": [],    \"location_landmark\": []  },  \"person\": [    \"UGVy\"  ],  \"products\": [],  \"provinceNameEn\": \"\",  \"provinceNameZh\": \"\",  \"pubTime\": 1491098732000,  \"pubdate\": \"2017-04-02 10:05:32\",  \"pv\": 0,  \"sensitiveType\": 0,  \"sentimentId\": 1,  \"similarityId\": \"\",  \"tagId\": 16,  \"titleEn\": \"Board plant project, Sweden\",  \"titleSrc\": \"Board plant project, Sweden\",  \"titleZh\": \"厂 项目 、 瑞典\",  \"transFromM\": \"\",  \"transfer\": 0,  \"updated\": \"2017-04-02 10:16:47\",  \"url\": \"http://engineeringnews.co.za/login.php?url=/article/board-plant-project-sweden-2017-03-31\",  \"userTag\": \"\",  \"uuid\": \"4714910987336470A873FC30CD3DE88\",  \"view\": 0,  \"websiteId\": \"\" }";
//  	
//    	try{
//    	InsertDB idb = new InsertDB();
//        Map maps=idb.parseJson(soc_str);
//        String sql = idb.getTabSql(maps,"soc");
//        System.out.println("sql1="+sql);
//        
//        maps=idb.parseJson(new_str);  
//        sql = idb.getTabSql(maps,"news");
//        System.out.println("sql2="+sql);
//        
//    	}catch(Exception e){
//    		System.out.println(e.toString());
//    	}
//    	
//    	String str="MADRID,\"aaa\" , 'bbb' ";
//    	str = str.replaceAll("\'","\\'");
//    	str = str.replaceAll("\"","\\\"");
//    	//str = str.replaceAll("aaa","xxx");
//    	System.out.println("str="+str);
//    }
//*/
//    public  int doMsg( Connection conn,String mt, String str)    {
//    	
//        //String str = "{\"a\":\"zhangsan\",\"b\":\"lisi\",\"c\":\"wangwu\",\"d\":\"maliu\"}";
//  	    int ret=0;
//    	try{
//    		
//    		Map maps = parseJson(str);
//    		
//    		String sql="";
//    		if(maps != null){
//    			sql = getTabSql(maps,mt);
//    			//System.out.println("sql="+sql);
//        		ret = insertDB(conn,sql);
//
//    		}else{
//    			System.out.println("Error Format jsonstr=["+str+"]");
//    		}
//    		
//    	}catch(Exception e){
//    		System.out.println("msg error=["+e.toString()+"]");
//    	}
//    	return ret;
//    }
//    
//    
//    public int insertDB( Connection conn,String sql) throws Exception {
//    
//        int result=0;
//        
//        if(sql == null || sql.isEmpty() ){
//            System.out.println("invalid input param!");
//        	return -1;
//        }
//        
//        try {
//        	
//            Statement stmt = conn.createStatement();
//            result = stmt.executeUpdate(sql);
//
//        } catch (SQLException e) {
//            System.out.println("MySQL");
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } 
//        return result;
//    }
 
}

