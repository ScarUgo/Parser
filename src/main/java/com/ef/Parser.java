package com.ef;

import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.ef.ParseController.Duration;

public class Parser {
	
	private static String startDateInput = "";
	private static String durationInput = "";
	private static String thresholdInput = "";
	
	private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss");
	
	
	public static void main(String[] args) {
		        
        ParseController parseController = new ParseController();

		try {
			
			validateInput(args);
			
			LocalDateTime startDate = LocalDateTime.parse(startDateInput, formatter);
			int threshold = Integer.valueOf(thresholdInput);
			Duration durationType = Duration.valueOf(durationInput);
		
			parseController.intializeDatabaseAccess();
			parseController.createRequestTable();
			parseController.loadLogFile();
			parseController.createFilterTable();
			ResultSet resultSet = parseController.filterLogs(startDate, durationType, threshold);
			parseController.outputFilteredResults(resultSet);

		} 
		catch (Exception e) {
			e.printStackTrace(); //TODO throw error
		}
	}
	
	private static void validateInput(String[] args){
				
		for(int i = 0; i < args.length; i++){
			System.out.println("args[i] >> " + args[i]);
			
			String[] keyValuePair = args[i].split("=");
			System.out.println("key >> " + keyValuePair[0]);
			System.out.println("value >> " + keyValuePair[1]);
			System.out.println("done");
			
			switch (keyValuePair[0]) {
			case "--startDate":
				startDateInput = keyValuePair[1];
				break;
				
			case "--threshold":
				thresholdInput = keyValuePair[1];
				break;
				
			case "--duration":
				durationInput = keyValuePair[1];
				break;

			default:
				throw new IllegalArgumentException("Argument not recognized: " + keyValuePair[0]);
			}
		}
	}

}
