package k2Studio.lu.DVD;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class syncRefTask implements Runnable  {
	static Logger log = LoggerFactory.getLogger(LUType.class.getName());
	String tableName;
	Statement stmt = null;
	Connection conn = null;
	Connection cassConn = null;
	String luName = null;
	boolean pass = true;
	
	
	public syncRefTask(String tableName, String luName) {
		this.tableName = tableName;
		this.luName = luName;
	}
    	
	public void run() {
		
		//Start time
		long startTime = System.nanoTime();
		String threadName = Thread.currentThread().getName();
		//ESTABLISHING CONNECTION TO FABRIC 
		String connectioURLToFabric = "jdbc:k2view://localhost:9042?loadbalancing=com.k2view.cdbms.policy.SingleNodePolicy('localhost')";
		try {
			conn = DriverManager.getConnection(connectioURLToFabric, "admin", "admin"); 
			log.info("================== MULTI THREAD PARSER - ESTABLISH CONNECTION SUCCESFULLY WITH THREAD " + threadName + "==================");          
		} catch (SQLException e) {
		    log.error("================== MULTI THREAD PARSER - FAILED TO ESTABLISH CONNECTION WITH THREAD   " + threadName + " ==================");
		}
		
		//ESTABLISHING CONNECTION TO CASSANDRA
		String connectioURLToCass = "jdbc:cassandra://localhost:9042?consistency=QUORUM&loadbalancing=com.k2view.cdbms.policy.SingleNodePolicy('localhost')";
		try {
			cassConn = DriverManager.getConnection(connectioURLToCass, "admin", "admin"); 
			log.info("================== MULTI THREAD PARSER - ESTABLISH CONNECTION SUCCESFULLY! \nTHREAD " + threadName + "==================");          
		} catch (SQLException e) {
		    log.error("================== MULTI THREAD PARSER - FAILED TO ESTABLISH CONNECTION! \nTHREAD   " + threadName + " ==================");
		}
		
		try {
			updateStartTime(cassConn, tableName);	
			ResultSet rs = conn.createStatement().executeQuery("select count(*)  from "+tableName);
			String tableCount = rs.getString(1);
			rs.close();
			conn.createStatement().execute("set default");
			log.info("================== MULTI THREAD PARSER - TABLE " + tableName + " WAS DONE SYNCING SUCCESFULLY! \nTHREAD " + threadName + "\n TOTAL TABLE COUNT IS - " + tableCount +"==================");
			updateEndTime(cassConn, tableName, tableCount, startTime);
		} catch (Exception e) {			
			updateEndTimeOnFailure(cassConn, tableName, e.getMessage(), startTime);	
			log.error("================== MULTI THREAD PARSER - TABLE " + tableName + " WAS FAILED TO SYNC! \nTHREAD " + threadName + "==================");
			e.printStackTrace();
		}finally{
			try{
				conn.close();
				log.info("================== MULTI THREAD PARSER - CONNECTION TO FABRIC WAS CLOSED! \nTHREAD " + threadName + "==================");
				}catch(SQLException e){
					log.error("================== MULTI THREAD PARSER - CONNECTION TO FABRIC WAS FAILED TO CLOSED! \nTHREAD " + threadName+ "==================");			
					e.printStackTrace();}
			}
	}

	public void updateStartTime(Connection cassConn, String tableName){
		String threadName = Thread.currentThread().getName();
		try{
			PreparedStatement preStmt2 = cassConn.prepareStatement("insert into k2view_" + luName + ".ref_log (ref_table_name, start_time, status, end_time, ref_load_time, error_msg, num_of_rows_loaded, thread_name) values (?,?,?,?,?,?,?,?)");
			preStmt2.setString(1,tableName);preStmt2.setString(2,currentTime());preStmt2.setString(3,"Running");preStmt2.setString(4,null);preStmt2.setString(5,null);preStmt2.setString(6,null);preStmt2.setString(7,null);preStmt2.setString(8,threadName);
			preStmt2.execute();
		} catch (SQLException ex) {	
			log.error("================== MULTI THREAD PARSER - ERROR ACCURED WHILE UPDATING REF LOG TABLE! \nTHREAD " + threadName + "\nFUNCTION NAME - updateStartTime ==================");
			ex.printStackTrace();
		}			
	}
	
	public void updateEndTime(Connection cassConn, String tableName, String num_of_rows_loaded, long startTime){
			String threadName = Thread.currentThread().getName();
		try{
			String parserDuration = (System.nanoTime() - startTime)/1000000000.0+"";
			PreparedStatement preStmt2 = cassConn.prepareStatement("insert into k2view_" + luName + ".ref_log (ref_table_name, end_time, status, ref_load_time, error_msg, num_of_rows_loaded) values (?,?,?,?,?,?)");
			preStmt2.setString(1,tableName);preStmt2.setString(2,currentTime());preStmt2.setString(3,"Finished");preStmt2.setString(4,parserDuration);preStmt2.setString(5,null);preStmt2.setString(6,num_of_rows_loaded);
			preStmt2.execute();
		} catch (SQLException ex) {	
			log.error("================== MULTI THREAD PARSER - ERROR ACCURED WHILE UPDATING REF LOG TABLE! \nTHREAD " + threadName + "\nFUNCTION NAME - updateEndTime ==================");
			ex.printStackTrace();
		}finally{
			try{
				cassConn.close();
				log.info("================== MULTI THREAD PARSER - CONNECTION TO CASSANDRA WAS CLOSED! \nTHREAD " + threadName + "==================");
				}catch(SQLException e){
					log.error("================== MULTI THREAD PARSER - CONNECTION TO CASSANDRA WAS FAILED TO CLOSED! \nTHREAD " + threadName+ "==================");			
					e.printStackTrace();}
			}		
	}
	
	public void updateEndTimeOnFailure(Connection cassConn, String tableName, String error_msg, long startTime){
			String threadName = Thread.currentThread().getName();
		try{
			String parserDuration = (System.nanoTime() - startTime)/1000000000.0+"";
			PreparedStatement preStmt2 = cassConn.prepareStatement("insert into k2view_" + luName + ".ref_log (ref_table_name, end_time, status, ref_load_time, error_msg, num_of_rows_loaded) values (?,?,?,?,?,?)");
			preStmt2.setString(1,tableName);preStmt2.setString(2,currentTime());preStmt2.setString(3,"Failed");preStmt2.setString(4,parserDuration);preStmt2.setString(5,error_msg);preStmt2.setString(6,null);
			preStmt2.execute();
		} catch (SQLException ex) {	
			log.error("================== MULTI THREAD PARSER - ERROR ACCURED WHILE UPDATING REF LOG TABLE! \nTHREAD " + threadName + "\nFUNCTION NAME - updateEndTimeOnFailure ==================");
			ex.printStackTrace();
		}finally{
			try{
				cassConn.close();
				log.info("================== MULTI THREAD PARSER - CONNECTION TO CASSANDRA WAS CLOSED! \nTHREAD " + threadName + "==================");
				}catch(SQLException e){
					log.error("================== MULTI THREAD PARSER - CONNECTION TO CASSANDRA WAS FAILED TO CLOSED! \nTHREAD " + threadName+ "==================");			
					e.printStackTrace();}
			}		
	}
	
	public String currentTime(){
		java.text.DateFormat dateFormat = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		java.util.Date date = new java.util.Date();
		return dateFormat.format(date);
	}

}