package com.ef;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParseControllerTest {
	
	ParseController parseController;	
	
	@Before 
	public void initializeDatabase(){
		
		//ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream databasePropertiesStream = classLoader.getResourceAsStream("db.properties");
		
		parseController = new ParseController();
		parseController.initializeDatabase(databasePropertiesStream);
		parseController.createRequestTable("test_logEntry");	
		
		
		Properties databaseProperties = new Properties();	
		
		try{			
			databaseProperties.load(databasePropertiesStream);			
		}
		catch(Exception e){
			e.printStackTrace();
		}		
				
	}

	@Test
	public void testLoadFile(){		
		
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream logFileStream = classLoader.getResourceAsStream("access_log");
		
		parseController.loadLogFile(logFileStream);
		
		try{
			ResultSet resultSet = parseController.getLogEntryCount();
						
			if(resultSet.next()){
				int num = Integer.valueOf(resultSet.getString("num"));				
				assertEquals(28, num);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	@After
	public void shutDownDatabase(){
		
	}
}
