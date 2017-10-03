# Parser
Parses a web server log file using the pipe delimiter, writes log entries to MySQL db and filters out IPs that have exceeded a threshold

SOURCE
1. Maven "package" goal generates the parser.jar tool to the /target directory
2. For unit tests, database connection parameters are set in "src/test/resources/db.properties"

RUNNING
1. File to be parsed must have the name "requests.log" and must be in the same directory as parser.jar 
2. Database connection parameters are set in the "db.properties" file. Modify as necessary
3. To run the parser tool, execute the following command while modifying the input variables:
	java -cp "parser.jar" com.ef.Parser --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100
