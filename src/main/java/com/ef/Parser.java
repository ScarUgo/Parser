package com.ef;

import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Entry point of the Parser application. 
 * Parser.java uses the utility class ParseController.java to read a log file and write outputs to database and console
 * Parser.java takes three input parameters: startDate, duration and threshold
 */
public class Parser {
	
	/**Input parameters read from console*/
	private static LocalDateTime startDate;
	private static Duration duration;
	private static int threshold;
	
	/**Defined duration types*/
	enum Duration{
		hourly, daily
	}
	
	/** 
	 * Used to ensure proper formatting of the startDate input
	 */
	private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss");
	
	/**
	 * Application entry point
	 * @param args
	 */
	public static void main(String[] args) {
		
        ParseController parseController = new ParseController(); // Utility class for accessing core functions

		try {
			
			parseCommandlineParameters(args); 					//Step 1: Validate the three input parameters entered in console			
			parseController.createRequestTable("log_request");	//Step 2: Create log_request table
			parseController.loadLogFile("access_log"); 			//Step 3: Read the log file entries and write to log_request table
			parseController.createFilterTable("filter_result");	//Step 4: Create the filter_result table			
			ResultSet resultSet = parseController				
					.filterLogs(startDate, duration, threshold);//Step 5: Filter the results using input parameters 
			parseController.outputFilteredResults(resultSet);	//Step 6: Write filter result to filter_result table and console

		} 
		catch (Exception e) {
			e.printStackTrace(); //TODO throw error
		}
	}
	
	/**
	 * This function reads the command line arguments and validates them.
	 * This function updates the class variables used in the main() function
	 * 
	 * @param args The command line arguments to be parsed and validated
	 * @return Returns void
	 */
	private static void parseCommandlineParameters(String[] args){//TODO should throw validation exceptions
		
		// Loop through the command line arguments, split each using the '=' delimiter
		// Pass the second array element(i.e. the value of the argument) to input variables of this Parser.java class
		for(int i = 0; i < args.length; i++){
			String[] keyValuePair = args[i].split("=");
			
			//Check each input variable against predefined keys
			switch (keyValuePair[0]) {
			case "--startDate":
				startDate = LocalDateTime.parse(keyValuePair[1], formatter);
				break;
				
			case "--threshold":
				threshold = Integer.valueOf(keyValuePair[1]);
				break;
				
			case "--duration":
				duration = Duration.valueOf(keyValuePair[1]);
				break;

			default:
				throw new IllegalArgumentException("Argument not recognized: " + keyValuePair[0]);
			}
		}
	}

}
