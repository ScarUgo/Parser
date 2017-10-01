package com.ef;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DatabaseAccess {
	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;

	public MysqlDataSource getMySQLDataSource() throws FileNotFoundException, IOException {

		Properties props = new Properties();
		//FileInputStream fis = new FileInputStream("src/main/resources/db.properties");
		FileInputStream fis = new FileInputStream("db.properties");
		MysqlDataSource ds = new MysqlConnectionPoolDataSource();

		props.load(fis);

		ds.setURL(props.getProperty("mysql.url"));
		ds.setUser(props.getProperty("mysql.username"));
		ds.setPassword(props.getProperty("mysql.password"));

		return ds;
	}

	public void writeLogEntriesToDatabase(String ipAddress, Date date, String fullText) {
		try {
			connection = this.getMySQLDataSource().getConnection();
			statement = connection.createStatement();

			preparedStatement = connection.prepareStatement("INSERT INTO  parser.ipaddress VALUES (default, ?, ?, ?)");

			preparedStatement.setString(1, ipAddress);
			preparedStatement.setTimestamp(2, new java.sql.Timestamp(date.getTime()));
			preparedStatement.setString(3, fullText);
			
			preparedStatement.executeUpdate();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}
	
	public void updateFilterTable(String ipAddress, String comments) {
		try {
			connection = this.getMySQLDataSource().getConnection();
			statement = connection.createStatement();

			preparedStatement = connection.prepareStatement("INSERT INTO  parser.filter_results VALUES (default, ?, ?)");

			preparedStatement.setString(1, ipAddress);
			preparedStatement.setString(2, comments);
			
			preparedStatement.executeUpdate();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createTable(String tableName) {
		try {
			connection = this.getMySQLDataSource().getConnection();
			statement = connection.createStatement();
			
			statement.execute("DROP TABLE IF EXISTS parser."+tableName);
			
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
	
	
	public void createFilterTable(String tableName) {
		try {
			connection = this.getMySQLDataSource().getConnection();
			statement = connection.createStatement();
			
			statement.execute("DROP TABLE IF EXISTS parser."+tableName);
			
			statement.execute("CREATE TABLE parser."+ tableName +" (" + "id BIGINT(20) NOT NULL AUTO_INCREMENT," + 
					"ip_address VARCHAR(255) NULL,"+ 
					"comments VARCHAR(500) NULL," + "PRIMARY KEY (id))");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}
	
	public ResultSet fetchThresholdRequests(LocalDateTime startTime, LocalDateTime endTime, int threshold) {
		try {		
						
			connection = this.getMySQLDataSource().getConnection();
						
			preparedStatement = connection.prepareStatement("SELECT ip_address, COUNT(ip_address) AS 'num' " +
										" FROM parser.ipaddress " + 
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
	
	public void writeFilteredResultsToDB(ResultSet resultSet) {
		
		try{ 
			System.out.println("inside writeResults >> " + resultSet.getFetchSize());
			
			// ResultSet is initially before the first data set
			while (resultSet.next()) {				
				String ipAddress = resultSet.getString("ip_address");
				String num = resultSet.getString("num");
				System.out.println("ipAddress: " + ipAddress + " num: " + num);
				
				String comment = "The ip has exceeded threshold of x by making " + num + " requests";
				
				updateFilterTable(ipAddress, comment);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}

	// Close the resultSet and database connection
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
		} catch (Exception e) {

		}
	}
}
