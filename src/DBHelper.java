import java.sql.*;


public class DBHelper {	
	private java.sql.Connection conn;
	private Statement stmt;
	
	public DBHelper() {
		/*
		setupConnection();
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	/*
	 * Update the sentiment value for a particular tweet
	 */
	public void updateSentiment(long tweetId, double sentiment) {
		if (conn == null) setupConnection();
		try {
			if (stmt == null) stmt = conn.createStatement();
			String update = "UPDATE Tweets SET sentiment = " + sentiment 
					+ " WHERE tweet_id = " + tweetId;
			stmt.executeUpdate(update);
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
			
		}
	}
	
	public void setupConnection() {	  
		String dbName   = DBInfo.dbName;
		String userName = DBInfo.userName;
		String password = DBInfo.password;
		String hostname = DBInfo.hostname;
		String port     = DBInfo.port;
		
		String jdbcUrl = "jdbc:mysql://" + hostname + ":" +
				port + "/" + dbName + "?user=" + userName + "&password=" + password;
		try {
			conn = DriverManager.getConnection(jdbcUrl);
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} 
	}
	
	public void closeConnection() {
		if (stmt != null) try { stmt.close(); } catch (SQLException ignore) {}
		if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
	}
}
