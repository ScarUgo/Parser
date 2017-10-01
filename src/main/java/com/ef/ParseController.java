package com.ef;

import java.io.Console;
import java.io.File;
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

public class ParseController {
	
	private static Pattern pattern;
	private static Matcher matcher;

	enum Duration{
		hourly, daily
	}

	// private static final String IPADDRESS_PATTERN =
	// "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
	// "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
	// "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
	// "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

	private static final String ipv4Pattern = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
	private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

	/**
	 * Validate ip address with regular expression
	 * 
	 * @param ip
	 *            ip address for validation
	 * @return true valid ip address, false invalid ip address
	 */
	public boolean validate(final String ip) {
		matcher = pattern.matcher(ip);
		return matcher.matches();
	}

	DatabaseAccess dbAccess;
	
	public void intializeDatabaseAccess() {		
		dbAccess = new DatabaseAccess();
	}
	
	public void createRequestTable() {
		dbAccess.createTable("ipaddress");
	}
	
	public void createFilterTable(){
		dbAccess.createFilterTable("filter_results");
	}

	public void loadLogFile() {
		try {
			Scanner scanner = new Scanner(new File("access_log"));

			String newLineDelimiter = "\n";
			String pipeDelimiter = "|";

			scanner.useDelimiter(newLineDelimiter);

			List<String> logEntries = new ArrayList<>(); //TODO Only necessary if final report is required

			while (scanner.hasNext()) {

				String logEntry = scanner.next();

				String ipAddress = checkLogForIPAddress(logEntry);
				Date date = checkLogForTime(logEntry);

				dbAccess.writeLogEntriesToDatabase(ipAddress, date, logEntry); //TODO opening and closing of database connection in loop?

				logEntries.add(logEntry);

				System.out.println(scanner.next()); //TODO for debugging
				System.out.println("log entry >> " + logEntries.size());
			}
			scanner.close();

			System.out.println("Array size >> " + logEntries.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Date checkLogForTime(String logEntry) {
		
		Date foundDate = null;
		
		// SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd HH:MM:SS");
		SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss"); //TODO

		int startingIndex = 0;

		ParsePosition parsePostion = new ParsePosition(startingIndex);

		for (int i = 0; i < logEntry.length(); i++) {
			parsePostion.setIndex(i);
			foundDate = format.parse(logEntry, parsePostion);

			if (foundDate != null)
				break;
		}

		if (foundDate == null) {
			// TODO No date found, throw exception?
		}

		return foundDate;
	}

	public String checkLogForIPAddress(String logEntry) {

		String foundIPAddress = "";

		pattern = Pattern.compile(ipv4Pattern);//TODO

		Matcher matcher = pattern.matcher(logEntry);

		if (matcher.find()) {
			foundIPAddress = matcher.group();
		} else {
			System.out.printf("No match found.%n");
			// TODO No IP found, throw exception?
		}

		return foundIPAddress;
	}
	
	
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
	
	public void outputFilteredResults(ResultSet resultSet){
		dbAccess.writeFilteredResultsToDB(resultSet);
	}

	public void checkConsoleForIPAddress() {
		pattern = Pattern.compile(ipv4Pattern);

		Console console = System.console();

		if (console == null) {
			System.err.println("No console.");
			System.exit(1);
		}
		while (true) {

			Matcher matcher = pattern.matcher(console.readLine("Enter input IP to search: "));

			boolean found = false;
			while (matcher.find()) {
				console.format("I found the IP" + " \"%s\" starting at " + "index %d and ending at index %d.%n",
						matcher.group(), matcher.start(), matcher.end());
				found = true;
			}
			if (!found) {
				console.format("No match found.%n");
			}
		}

	}

}
