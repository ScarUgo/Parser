package com.ef;

import java.io.InputStream;
import java.sql.ResultSet;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ef.Parser.Duration;

/**
 * Controller class for the Parser application. 
 * The core functions of the application are implemented in this class
 */
public class ParseController {
	
	private Pattern pattern;
	private DatabaseAccess dbAccess;
	
	private String newLineDelimiter = "\n";
	private String pipeDelimiter = "|";
	
	// SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd.HH:MM:SS");
	private SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
	
	
	private final String ipv4Pattern = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
	
	
	/**
	 * Creates an instance of the database connection object
	 */
	public ParseController(){		
		pattern = Pattern.compile(ipv4Pattern); //This only needs to be compiled once the class is instantiated
	}
	
	/**
	 * Initializes database access
	 * @param
	 */
	public void initializeDatabase(InputStream databePropertiesStream){
		dbAccess = new DatabaseAccess();
		dbAccess.initializeDatabaseAccess(databePropertiesStream);
	}
	
	/**
	 * Creates the main table where all log entries are stored
	 * @param The name of the database table to be created
	 */
	public void createRequestTable(String mainTableName) {
		dbAccess.createMainTable(mainTableName);
	}
	
	/**
	 * Deletes database table with specified name
	 * @param The name of the database table to be deleted
	 */
	public void deleteTable(String tableName) {
		dbAccess.deleteTable(tableName);
	}
	
	/**
	 * Creates the filter table where filtered log entries are stored
	 * @param The name of the database table to be created
	 */
	public void createFilterTable(String filterTableName){
		dbAccess.createFilterTable(filterTableName);
	}
	
	/**
	 * Reads the log file from disk, parses the file using delimiter and writes contents to main table
	 * @param logFileName The name of the log file
	 */
	public void loadLogFile(InputStream logFileName) {
		try {
			//Scanner scanner = new Scanner(new File(logFileName));
			Scanner scanner = new Scanner(logFileName);
			scanner.useDelimiter(newLineDelimiter);
			List<String> logEntries = new ArrayList<>(); //TODO Only necessary if final report is required
			
			System.out.println("Starting file parsing ... ");

			while (scanner.hasNext()) {

				String logEntry = scanner.next();

				String ipAddress = checkLogForIPAddress(logEntry);
				Date date = checkLogForTime(logEntry, this.dateFormat);

				dbAccess.writeLogEntryToMainTable(ipAddress, date, logEntry); //TODO opening and closing of database connection in loop?

				logEntries.add(logEntry);
			}
			scanner.close();

			System.out.println("... done with file parsing. Log entry count is: " + logEntries.size());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Parses the input string and searches for a time according to defined format
	 * @param logEntry The input string to be parsed
	 * @param dateFormat Predefined {@link SimpleDateFormat} format
	 * @return A {@link Date} object if one is found
	 */
	private Date checkLogForTime(String logEntry, SimpleDateFormat dateFormat) {
		
		Date foundDate = null;
		
		ParsePosition parsePostion = new ParsePosition(0);

		for (int i = 0; i < logEntry.length(); i++) {
			parsePostion.setIndex(i);
			foundDate = dateFormat.parse(logEntry, parsePostion);

			if (foundDate != null)
				break;
		}

		if (foundDate == null) {
			// TODO No date found, throw exception?
		}

		return foundDate;
	}
	
	/**
	 * Parses the input string and searches for a IP address
	 * @param logEntry The input string to be parsed
	 * @return An IP address string if one is found
	 */
	private String checkLogForIPAddress(String logEntry) {

		String foundIPAddress = "";

		Matcher matcher = pattern.matcher(logEntry);

		if (matcher.find()) {
			foundIPAddress = matcher.group();
		} 
		else {
			System.out.printf("No match found.%n"); // TODO No IP found, throw exception?			
		}

		return foundIPAddress;
	}
	
	/**
	 * Uses the input parameters to filter IP addresses that meet the criteria
	 * @param startTime The start time of log entries to be filtered
	 * @param duration The period after start time for log entries to be filtered
	 * @param threshold The minimum amount of occurrence of the IP address to be filtered
	 * @return A {@link ResultSet} of IP addresses that match the input criteria
	 */
	public ResultSet filterLogs(LocalDateTime startTime, Duration duration, int threshold) {

		LocalDateTime endTime = null;

		if (duration == Duration.hourly) {
			endTime = startTime.plusHours(1); // Add just one hour
		} 
		else if (duration == Duration.daily) {
			endTime = startTime.plusDays(1); // Add just one day
		}

		return dbAccess.fetchThresholdRequests(startTime, endTime, threshold);
	}
	
	/**
	 * Gets a count of all log entries
	 * @return
	 */
	public ResultSet getLogEntryCount() {
		return dbAccess.fetchCountOfLogEntries();
	}
	
	
	/**
	 * Output the filtered results to both database and console
	 * @param The {@link ResultSet} to be outputed
	 */
	public void outputFilteredResults(ResultSet resultSet){
		dbAccess.writeFilteredResultsToDB(resultSet);
	}
}
