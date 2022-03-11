package com.transglobe.kafka.connect.oracle;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleSqlUtils2 {

	static final Logger log = LoggerFactory.getLogger(OracleSqlUtils2.class);    

	public OracleSqlUtils2(){

	}

	public static Boolean getLogFilesV2(Connection conn,Long currScn) throws SQLException{        
        int i = 0;
        String option;
        List <String> logFilesBase = new ArrayList<String>();
        List <String> logFilesLogmnr = new ArrayList<String>();
        String sqlBase = OracleConnectorSQL2.LOGMINER_LOG_FILES_LOG$;
        sqlBase = sqlBase.replace(":vcurrscn",currScn.toString());
        PreparedStatement ps = conn.prepareCall(sqlBase);
        log.info("################################# Scanning Log Files for SCN :{}",currScn);
        ResultSet rs = ps.executeQuery();
        while (rs.next()){
            log.info("Base log files {}",rs.getString("NAME"));
            logFilesBase.add(rs.getString("NAME"));
        }
        rs.close();
        ps.close();

        ps = conn.prepareCall(OracleConnectorSQL2.LOGMINER_LOG_FILES_LOGMNR$);
        rs = ps.executeQuery();
        while (rs.next()){
            log.info("logmnr_logs log files {}",rs.getString("NAME"));
            logFilesLogmnr.add(rs.getString("NAME"));
        }

        if (!logFilesBase.equals(logFilesLogmnr)){
            ListIterator<String> iterator = logFilesBase.listIterator();
            while (iterator.hasNext()){
                String logFile = iterator.next();                
                log.info("Log file will be mined {}",logFile);
                if (i==0){
                    option = "DBMS_LOGMNR.NEW";
                    i++;
                }else {
                    option = "DBMS_LOGMNR.ADDFILE";
                }            
                executeCallableStmt(conn, OracleConnectorSQL2.LOGMINER_ADD_LOGFILE.replace(":logfilename",logFile).replace(":option", option));                
            }
        }
        log.info("#################################");
        rs.close();
        ps.close();        
        return i>0 ? true : false;        
    }

    public static Boolean getLogFiles(Connection conn,String sql,Long currScn) throws SQLException{        
        int i = 0;
        String option;
        List<String> logFiles=null;
        String pSql = sql.replace(":vcurrscn",currScn.toString());        
        PreparedStatement ps = conn.prepareCall(pSql);
        log.info(pSql);
        log.info("################################# Scanning Log Files for SCN :{}",currScn);
        ResultSet rs = ps.executeQuery();
        while (rs.next()){
            logFiles = Arrays.asList(rs.getString("NAME").split(" "));
        }
        if (logFiles != null){
            ListIterator<String> iterator = logFiles.listIterator();
            while (iterator.hasNext()){
                String logFile = iterator.next();
                log.info("Log file will be mined {}",logFile);
                if (i==0){
                    option = "DBMS_LOGMNR.NEW";
                    i++;
                }else {
                    option = "DBMS_LOGMNR.ADDFILE";
                }            
                executeCallableStmt(conn, OracleConnectorSQL2.LOGMINER_ADD_LOGFILE.replace(":logfilename",logFile).replace(":option", option));
            }
        }

        log.info("#################################");
        rs.close();
        ps.close();

        return i>0 ? true : false;
    }
    
    public static void executeCallableStmt(Connection conn,String sql) throws SQLException{        
        CallableStatement s = conn.prepareCall(sql);
        s.execute();
        s.close();
    }

    public static Long getCurrentScn(Connection conn) throws SQLException{
        Long currentScn=0L;
        PreparedStatement ps = conn.prepareCall(OracleConnectorSQL2.CURRENT_DB_SCN_SQL);
        ResultSet rs = ps.executeQuery();
        while (rs.next()){
            currentScn = rs.getLong("CURRENT_SCN");
        }
        rs.close();
        ps.close();
        return currentScn;
    }    
    public static void insertOffSet(Connection conn, String connector, String applySync, boolean resetOffset, Long startScn, Long offsetScn, Long offsetCommitScn,String offsetRowId, boolean isOk) throws SQLException{
		PreparedStatement pstmt = null;
		String sql = null;
		try {
			sql = "insert into TM2_LOGMINER_OFFSET (START_TIME,CONNECTOR,APPLY_SYNC,RESET_OFFSET,START_SCN,OFFSET_SCN,OFFSET_COMMIT_SCN,OFFSET_ROW_ID,LOGMINER_STATUS,UPDATE_TIME) \n" + 
					"values (?,?,?,?,?,?,?,?,?,?)";
			Timestamp t =  new Timestamp(System.currentTimeMillis());
			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1,t);
			pstmt.setString(2, connector);
			pstmt.setString(3, applySync);
			pstmt.setInt(4, (resetOffset)? 1 : 0);
			pstmt.setLong(5, startScn);
			pstmt.setLong(6, offsetScn);
			pstmt.setLong(7, offsetCommitScn);
			pstmt.setString(8, offsetRowId);
			pstmt.setString(9, (isOk? "RUNNING" : "FAILED"));
			pstmt.setTimestamp(10, t);
			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}

	}    
    public static void updateLogminerReceived(Connection conn, Long scn, Timestamp heartbeat, String logminerClient) throws SQLException{

		CallableStatement cstmt = null;
		try {
			cstmt = conn.prepareCall("{call SP2_UPD_LOGMINER_RECEIVED(?,?,?,?)}");
			cstmt.setTimestamp(1, heartbeat);
			cstmt.setLong(2, scn);
			cstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			cstmt.setString(4,  logminerClient + " " + scn);
			cstmt.executeUpdate();
			cstmt.close();


		} finally {
			if (cstmt != null) cstmt.close();
		}
	}
    public static void updateLogminerStatus(Connection conn, boolean isOk) throws SQLException{

		CallableStatement cstmt = null;
		try {
			cstmt = conn.prepareCall("{call SP2_UPD_SERVER_STATUS(NULL,?,NULL)}");
			cstmt.setString(1, isOk? "RUNNING" : "FAILED");
			cstmt.executeUpdate();
			cstmt.close();


		} finally {
			if (cstmt != null) cstmt.close();
		}

	}

}