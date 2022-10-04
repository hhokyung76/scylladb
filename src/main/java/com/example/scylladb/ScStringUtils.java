package com.example.scylladb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;

public class ScStringUtils {
	private static final Logger logger = LoggerFactory.getLogger(ScStringUtils.class);
    public static void main(String[] args) {
    	if (true)
    	{
    		logger.info("fname = " + getFileUpperDir("C:\\testfile\\testPST.pst"));
    		logger.info("fname = " + getFileUpperDir("/testfile/testPST.pst"));
    		String sss = getNextExtFileGroupID("1234-1");
    		logger.info("sss = " + sss);
    		return;
    	}
    	logger.info("uuid = " + getObjectID("04"));
    	logger.info("uuid = " + getObjectID("04"));
    	//String gfsname = "C:\\testfile\\testPST.pst";
    	String gfsname = "/testfile/testPST.pst";
		String DirDtl = "/";
		//String DirDtl = ScStringUtils.GetDirDels(); 
		Integer rindex = gfsname.lastIndexOf(DirDtl);
		if (rindex > 0)
		{
			logger.info(Integer.toString(rindex));
			String oPath = gfsname.substring(0, rindex + 1);	
			String fname = gfsname.substring(rindex + 1, gfsname.length());	
			logger.info("sss " + oPath + " " +fname);
		}
		/*
		String osName = System.getProperty("os.name");// ("os.name"); //getProperty ("os.name");혻혻
		logger.info("osName "+ osName);
    	if (ScStringUtils.isWindow())
    	{
    		logger.info("Win");
    	}
    	else
    	{
    		logger.info("linux");
    	}
    	*/
    }
    // �뙆�씪紐낅쭔 媛��졇�삩�떎 
    public static String getFileName(String v_full_path)
    {
    	String s_next_id = null;
    	String DelDir = GetDirDels();
    	Integer rindex = v_full_path.lastIndexOf(DelDir);
    	if (rindex > 0)
    	{
    		s_next_id = v_full_path.substring(rindex+1); 		
    	}
    	else
    	{
        	rindex = v_full_path.lastIndexOf("\\");
        	if (rindex > 0)
        	{
        		s_next_id = v_full_path.substring(rindex+1); 		
        	}
        	else
        	{
        		s_next_id = "#";
        	}
    	}
    	return s_next_id;	
    }
    // 洹명뙆�씪�쓽 �뵒�젆�넗由щ��  媛��졇�삩�떎 
    public static String getFileDir(String v_full_path)
    {
    	String s_next_id = null;
    	String DelDir = GetDirDels();
    	Integer rindex = v_full_path.lastIndexOf(DelDir);
    	if (rindex > 0)
    	{
    		s_next_id = v_full_path.substring(0, rindex+1); 		
    	}
    	else
    	{
    		s_next_id = "#";
    	}
    	return s_next_id;	
    }  
    // 洹명뙆�씪�쓽 �긽�쐞 �뵒�젆�넗由щ��  媛��졇�삩�떎 
    public static String getFileUpperDir(String v_full_path)
    {
    	String s_next_id = null;
    	String DelDir = GetDirDels();
    	Integer rindex = v_full_path.lastIndexOf(DelDir);
    	if (rindex < 0)
    	{
    		return "#";
    	}
    	String stemp = v_full_path.substring(0, rindex);
    	rindex = stemp.lastIndexOf(DelDir);
    	if (rindex > 0)
    	{
    		s_next_id = v_full_path.substring(0, rindex+1); 		
    	}
    	else
    	{
    		s_next_id = "#";
    	}
    	return s_next_id;	
    }

    // �닽�옄瑜� 鍮덉옄由ъ닔 0 梨꾩썙�꽌 留뚮뱾湲�
    public static String getSeqWithFormat(Integer seqno, String sformat)    
    {
        DecimalFormat df = new DecimalFormat(sformat);        
        String stmp = df.format(seqno);  	
        return stmp;
    }    
    // �뙆�씪 �솗�옣�옄 
    public static String getFileExt(String v_full_path)
    {
    	String s_next_id = null;
    	//String DelDir = GetDirDels();
    	Integer rindex = v_full_path.lastIndexOf(".");
    	if (rindex > 0)
    	{
    		s_next_id = v_full_path.toUpperCase().substring(rindex); 		
    	}
    	else
    	{
    		s_next_id = "";
    	}
    	return s_next_id;	
    }   
    // �떎�쓬 異붿텧 洹몃９ �븘�씠�뵒瑜� 留뚮뱺�떎 
    public static String getNextExtFileGroupID(String v_evdnc_extfl_group_id)
    {
    	String s_next_id = null;
    	Integer rindex = v_evdnc_extfl_group_id.indexOf("-");
    	if (rindex > 0)
    	{
    		String stemp =  v_evdnc_extfl_group_id.substring(rindex+1);
    		if (isStringInteger(stemp))
    		{
    			Integer tint = Integer.parseInt(stemp) + 1;    			
    			s_next_id = v_evdnc_extfl_group_id.substring(0, rindex+1) + Integer.toString(tint);
    		}
    		else
    		{
    			return v_evdnc_extfl_group_id;
    		}
    	}
    	else
    	{
    		s_next_id = v_evdnc_extfl_group_id + "-1";
    	}
    	return s_next_id;
    }
	/*
	 * tableid 04 -> �븬異뺥빐�젣�뵒�젆�넗由� 
	 * tableid 01 -> 異붿텧�뙆�씪 fileuuid   
	 */    
    public static boolean isStringInteger(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
      }

    /**
     * 04 鍮꾩젙�삎�뙆�씪
     * 05 �엫�떆�뵒�젆�넗由�
     * 06 �엫�떆異붿텧遺꾪븷�뙆�씪
     * 07 �씠硫붿씪eml
     * 08 pst�뿰�씫泥�
     * 09 pst�씪�젙.
     * 10 pst�뵒�젆�넗由�.
     * @param tableID
     * @return
     */
		
    public static String getObjectID(String tableID){
    	//�븬異뺥빐�젣 �뵒�젆�넗由щ뒗 吏㏐쾶 泥섎━�빐�빞 �븿. 
        UUID uuid = UUID.randomUUID();
        Random random = new Random();
        int rand = random.nextInt(99);
        String strRand =  Integer.toString(rand);
        if (rand < 10) strRand = "0" + Integer.toString(rand);

        String server_cd = "AT"; // 鍮꾩젙�삎 
        String objectID = server_cd + tableID + uuid.toString().replace("-", "")
                + strRand;
//        logger.info(objectID);
//        logger.info("length: " + objectID.length());
        return objectID;
    }
    
	public static boolean isWindow() 
	{
		String osName = null;
		osName = System.getProperty("os.name");// ("os.name"); //getProperty ("os.name");혻혻
		boolean bret;
	    if (osName.indexOf("Win") >= 0)
	    	bret = true;
	    else
	    	bret = false;
	    return bret;
	}	 
	// �뵒�젆�넗由ш뎄遺꾩옄 
	public static String GetDirDels() 
	{
		String osName = null;
		osName = System.getProperty("os.name");// ("os.name"); //getProperty ("os.name");혻혻
		String bret;
	    if (osName.indexOf("Win") >= 0)
	    	bret = "\\";
	    else
	    	bret = "/";	    
	    return bret;
	}	 
	
	public static String getCurrentTime() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
		
		return dateStr;
	}
	

	public static String getCurrentTime2() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);
		
		return dateStr;
	}
	

	public static String convertDateToString(Date date) {
		String dateStr = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);
		
		return dateStr;
	}
	
	
	public static String getCurrentScheduleTime() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyy/MM/dd HH:mm").format(date);
		
		return dateStr+":00";
	}

	public static String getCurrentTimeOfLog() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
		
		return dateStr;
	}
	public static String getCurrentTimeOfLogId() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(date);
		
		return dateStr;
	}
	public static String getYYYYMMDD() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyyMMdd").format(date);
		
		return dateStr;
	}
	
	public static String getCurrentTimeHHmmss() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("HHmmss").format(date);
		
		return dateStr;
	}

	public static String getCurrentTimeFullDisplayHmmss() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
		
		return dateStr;
	}

	public static String getCurrentTimeFullDisplayHmm() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("yyyyMMddHHmm").format(date);
		
		return dateStr;
	}
	

	public static String getCurrentTimeHHmmssSSS() {
		Date date = new Date();
		String dateStr = new SimpleDateFormat("HHmmss.SSS").format(date);
		
		return dateStr;
	}
	
	public static String transDateType(String yyyymmddHHssMM) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.KOREA);
		 
		String dateInString = "20200909141457";
		Date date = formatter.parse(dateInString);
		

		String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
		
		
		return dateStr;
		
	}
	
	public static Date CoinvertToDate(String dateTemp) throws ParseException {
		
		SimpleDateFormat fm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

		Date date = fm.parse(dateTemp);

		return date;
	}
	

}
