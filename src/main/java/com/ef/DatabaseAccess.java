package com.ef;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * Performs all CREATE, READ, UPDATE AND DELETE function on the database 
 */
public class DatabaseAccess {
	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	
	private String mainTableName;
	private String filterTableName;
	
	private MysqlDataSource dataSource;

	/**
	 * Loads a properties file and uses parameters from the file to create a connection to the database
	 * @param
	 * @throws IOException
	 */
	public void initializeDatabaseAccess(InputStream databasePropertiesStream){		
	
		Properties props = new Properties();		
		
		try{
			//FileInputStream fileInputStream = new FileInputStream("db.properties");		
			props.load(databasePropertiesStream);
			dataSource = new MysqlConnectionPoolDataSource();

			dataSource.setURL(props.getProperty("mysql.url"));
			dataSource.setUser(props.getProperty("mysql.username"));
			dataSource.setPassword(props.getProperty("mysql.password"));
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
		
	/**
	 * Private method that loads a properties file and uses parameters from the file to a connection to the database
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
//	private MysqlDataSource getMySQLDataSource() throws FileNotFoundException, IOException {
//		
//		ClassLoader loader = Thread.currentThread().getContextClassLoader();
//		InputStream resourceStream = loader.getResourceAsStream("db.properties");
//		
//		Properties props = new Properties();
//		props.load(resourceStream);
//				
//		MysqlDataSource datasource = new MysqlConnectionPoolDataSource();
//
//		datasource.setURL(props.getProperty("mysql.url"));
//		datasource.setUser(props.getProperty("mysql.username"));
//		datasource.setPassword(props.getProperty("mysql.password"));
//
//		return datasource;
//	}
	
	/**
	 * Writes log entry to filter table
	 * @param ipAddress
	 * @param comments
	 */
	private void writeLogEntryToFilterTable(String ipAddress, String comments) {
		try {
			connection = this.dataSource.getConnection();
			statement = connection.createStatement();

			preparedStatement = connection.prepareStatement("INSERT INTO  parser."+ filterTableName + " VALUES (default, ?, ?)");

			preparedStatement.setString(1, ipAddress);
			preparedStatement.setString(2, comments);
			
			preparedStatement.executeUpdate();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Writes a single log entry into main table
	 * @param ipAddress IP address
	 * @param date Time of log entry
	 * @param fullText Full log entry
	 */
	public void writeLogEntryToMainTable(String ipAddress, Date date, String fullText) {
		try {
			connection = this.dataSource.getConnection();
			statement = connection.createStatement();

			preparedStatement = connection.prepareStatement("INSERT INTO  parser."+ mainTableName + " VALUES (default, ?, ?, ?)");

			preparedStatement.setString(1, ipAddress);
			preparedStatement.setTimestamp(2, new java.sql.Timestamp(date.getTime()));
			preparedStatement.setString(3, fullText);
			
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//close();
		}
	}
	
	
	/**
	 * Creates main table
	 * @param tableName Name of table to be created
	 */
	public void createMainTable(String tableName) {
		this.mainTableName = tableName;
		try {
			connection = this.dataSource.getConnection();
			statement = connection.createStatement();
			
			statement.execute("DROP TABLE IF EXISTS parser." + mainTableName);
			
			statement.execute("CREATE TABLE parser."+ tableName +" (" + 
					"id BIGINT(20) NOT NULL AUTO_INCREMENT," + 
					"ip_address VARCHAR(255) NOT NULL," + 
					"access_date TIMESTAMP NOT NULL," + 
					"full_text VARCHAR(500) NOT NULL," + 
					"PRIMARY KEY (id))");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}

	}
	
	/**
	 * Create filter table
	 * @param tableName Name of table to be created
	 */
	public void createFilterTable(String tableName) {
		this.filterTableName = tableName;
		try {
			connection = this.dataSource.getConnection();
			statement = connection.createStatement();
			
			statement.execute("DROP TABLE IF EXISTS parser."+filterTableName);
			
			statement.execute("CREATE TABLE parser."+ tableName +" (" + "id BIGINT(20) NOT NULL AUTO_INCREMENT," + 
					"ip_address VARCHAR(255) NULL,"+ 
					"comments VARCHAR(500) NULL," + "PRIMARY KEY (id))");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}
	
	/**
	 * Deletes database table
	 * @param tableName Name of table to be deleted
	 */
	public void deleteTable(String tableName) {
		try {
			connection = this.dataSource.getConnection();
			statement = connection.createStatement();
			
			statement.execute("DROP TABLE IF EXISTS parser." + tableName);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}

	}
	
	/**
	 * Fetches IP Addresses based on input criteria
	 * @param startTime start time
	 * @param endTime end time
	 * @param threshold threshold
	 * @return The {@link ResultSet} of IP addresses and count of entries in main table
	 */
	public ResultSet fetchThresholdRequests(LocalDateTime startTime, LocalDateTime endTime, int threshold) {
		try {		
						
			connection = this.dataSource.getConnection();
						
			preparedStatement = connection.prepareStatement("SELECT ip_address, COUNT(ip_address) AS 'requestCount' " +
										" FROM parser."+ mainTableName + 
										" WHERE access_date >= '" + startTime + 
										"' AND" + 
										" access_date < '" + endTime +
										"' GROUP BY ip_address " + 
										" HAVING COUNT(ip_address) > " + threshold);
			
			resultSet = preparedStatement.executeQuery();
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//close();
		}
		
		return resultSet;
	}
	
	/**
	 * Fetches total count of IP Addresses
	 * @return The {@link ResultSet} of count of entries in main table
	 */
	public ResultSet fetchCountOfLogEntries() {
		try {			
			connection = this.dataSource.getConnection();
			
			preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS 'requestCount' FROM parser."+ mainTableName);
			
			resultSet = preparedStatement.executeQuery();
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//close();
		}
		
		return resultSet;
	}
	
	/**
	 * Write filtered results to filter table
	 * @param resultSet {@link ResultSet} from main table
	 */
	public void writeFilteredResultsToDB(ResultSet resultSet) {
				
		try{ 
			if(!resultSet.next())
				System.out.println("nothing!!");
			
			// ResultSet is initially before the first data set
			while (resultSet.next()) {				
				String ipAddress = resultSet.getString("ip_address");
				String requestCount = resultSet.getString("requestCount");
				
				String comment = "The ip " + ipAddress + " has exceeded threshold by making " + requestCount + " requests";
				
				System.out.println(comment); // output to console				
				writeLogEntryToFilterTable(ipAddress, comment); //output to database
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}

	/**
	 * Close database connections
	 */
	private void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
				System.out.println("Closing resultSet");
			}

			if (statement != null) {
				statement.close();
				System.out.println("Closing statement");
			}

			if (connection != null) {
				connection.close();
				System.out.println("Closing connection");
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
