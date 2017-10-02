package com.ef;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ef.Parser.Duration;

public class ParseControllerTest {
	
	static ParseController parseController;	
	static ClassLoader classLoader;
	static String mainTableName = "test_log_request";
	
	@BeforeClass 
	public static void initializeDatabase(){
		
		classLoader = ParseControllerTest.class.getClassLoader();
		InputStream databasePropertiesStream = classLoader.getResourceAsStream("db.properties");
		
		parseController = new ParseController();
		parseController.initializeDatabase(databasePropertiesStream);
		parseController.createRequestTable(mainTableName);		
	}

	@Test
	public void loadLogFileTest(){		
		
		InputStream logFileStream = classLoader.getResourceAsStream("requests.log");
		
		// Main method being tested
		parseController.loadLogFile(logFileStream); 
		
		ResultSet resultSet = parseController.getLogEntryCount(); 
		
		try{	
			if(resultSet.next()){
				int requestCount = Integer.valueOf(resultSet.getString("requestCount"));			
				
				//Check that the number of log entries parsed is equal to 28
				assertEquals(28, requestCount); 
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	@Test
	public void filterLogFilesTest(){
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss");
		
		LocalDateTime startTime = LocalDateTime.parse("2017-09-24.03:05:39", formatter);
		ResultSet resultSet = parseController.filterLogs(startTime, Duration.daily, 1);
						
		try{
			int rowcount = 0;
			if (resultSet.last()) {
			  rowcount = resultSet.getRow(); //move cursor to the last row and get the total row count
			  
			  assertEquals(5, rowcount); // test for the total row count
			  
			  resultSet.beforeFirst(); // moves the cursor back to the first element
			}
						
			//iterate through the result set and test that count of ip request matches expected count
			while (resultSet.next()) {
				
				String ipAddress = resultSet.getString("ip_address");
				int requestCount = Integer.valueOf(resultSet.getString("requestCount"));
				
				switch (ipAddress) {
				case "104.144.182.201":
					assertEquals(2, requestCount);
					break;
				case "173.44.167.191":
					assertEquals(2, requestCount);
					break;
				case "41.190.31.223":
					assertEquals(7, requestCount);
					break;
				case "62.210.203.97":
					assertEquals(2, requestCount);
					break;
				case "64.145.79.151":
					assertEquals(4, requestCount);
					break;
				default:
					break;
				}
			}			
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	@AfterClass
	public static void dropTestTable(){
		parseController.deleteTable(mainTableName); //Drop table
	}
}
