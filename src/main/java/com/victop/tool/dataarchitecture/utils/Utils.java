package com.victop.tool.dataarchitecture.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	private Logger logger = LoggerFactory.getLogger(Utils.class);
	
	public void insertDts(String schemaverguid,String dtsid,String dtstype,Connection H2Conn,Connection BizConn) throws Exception {
		ResultSet rs = null;
		try {
			String sql = "SELECT schemaverguid,tableguid FROM dstable " +
					" WHERE schemaverguid = '"+schemaverguid+"' AND tabletype = '交易表' AND tablelogicname = '"+dtsid+"'  AND dtstype = '"+dtstype+"'";

			rs = getrResultSetBySQL(sql, BizConn);
			String tableguid = "";
			while (rs.next()) {
				tableguid = rs.getString("tableguid");
			}
			close(rs);
			if(null != tableguid && !"".equalsIgnoreCase(tableguid) && !"null".equalsIgnoreCase(tableguid)){
				tableguid = UUID.randomUUID().toString();
			}
			insertDtssToDataArc(dtsid, tableguid, schemaverguid, H2Conn, BizConn);
			insertDtsTableColToDataArc(dtsid, tableguid, schemaverguid, H2Conn, BizConn);
		} catch (Exception e) {
			throw e;
		}finally{
			close(rs);
		}
	}
	
	public void insertDtsTableColToDataArc(String dtsid,String tableguid,String schemaverguid, 
			Connection H2Conn,Connection BizConn) throws SQLException {
		ResultSet rs = null;
		try {
			
			upDateBySQL("DELETE FROM dstableproperty WHERE schemaverguid = '"+schemaverguid+"' AND tableguid = '"+tableguid+"'", BizConn);
			
			rs = getrResultSetBySQL("SELECT propertyguid,schemaverguid FROM dstableproperty WHERE schemaverguid = '"+schemaverguid+"' AND tableguid = '"+tableguid+"'", BizConn);
			PreparedStatement pst3 = BizConn.prepareStatement("DELETE FROM dsproperty WHERE schemaverguid = ? AND propertyguid = ?");
			while (rs.next()) {
				pst3.setString(1, schemaverguid);
				pst3.setString(2, rs.getString("propertyguid"));
				pst3.addBatch();
			}
			pst3.executeBatch();
			close(pst3);
			close(rs);
			
			String insertDstableproperty = "INSERT INTO dstableproperty (schemaverguid,tableguid,propertyguid,iskey,isMain,caption,sequence)" +
					" VALUES (?,?,?,?,?,?)";
			String insertDsproperty = "INSERT INTO dsproperty (propertyguid,logicname,propertyname,propertytype,schemaverguid) " +
					" VALUES (?,?,?,?,?)";
			rs = getrResultSetBySQL("SELECT dtsid,columncaption,columnid,datatype,no,iskey,isMain,MasterName,propName FROM gmdtstablecolumn" +
					" WHERE dtsid = '"+dtsid+"'", H2Conn);
			PreparedStatement pst1 = BizConn.prepareStatement(insertDstableproperty);
			PreparedStatement pst2 = BizConn.prepareStatement(insertDsproperty);
			while (rs.next()) {
				String propertyguid = UUID.randomUUID().toString();

				String columncaption = rs.getString("columncaption");
				String columnid = rs.getString("columnid");
				String datatype = rs.getString("datatype");
				pst1.setString(1, schemaverguid);
				pst1.setString(2, tableguid);
				pst1.setString(3, propertyguid);
				pst1.setInt(4, rs.getInt("iskey"));
				pst1.setInt(5, rs.getInt("isMain"));
				pst1.setString(6, columncaption);
				pst1.setString(7, rs.getString("no"));
				pst1.addBatch();
				
				pst2.setString(1, propertyguid);
				pst2.setString(2, columncaption);
				pst2.setString(3, columnid);
				pst2.setString(4, datatype);
				pst2.setString(5, schemaverguid);
				pst2.addBatch();
			}
			pst1.executeBatch();
			close(pst1);
			pst2.executeBatch();
			close(pst2);
		} catch (SQLException e) {
			logger.error( "{}",e);
			throw e;
		}
	}
	
	public void insertDtssToDataArc(String dtsid,String tableguid,String schemaverguid,
			Connection H2Conn,Connection BizConn) throws SQLException {
		ResultSet rs = null;
		try {
			if(null != tableguid && !"".equalsIgnoreCase(tableguid) && !"null".equalsIgnoreCase(tableguid)){
				upDateBySQL("DELETE FROM dstable WHERE schemaverguid = '"+schemaverguid+"' AND tableguid = '"+tableguid+"'", BizConn);
			}
			String sql = "SELECT dtsid, dtstype, physicaltable, periodtype FROM gmmdldts WHERE dtsid = '"+dtsid+"'";
			rs = getrResultSetBySQL(sql, H2Conn);
			String insertStr = "INSERT INTO dstable (tablename,tabletype,tablelogicname,dtstype,tableguid,schemaverguid )" +
					" VALUES (?,?,?,?,?,?)";
			PreparedStatement pst = BizConn.prepareStatement(insertStr);
			while (rs.next()) {
				pst.setString(1, rs.getString("physicaltable"));
				pst.setString(2, "交易表");
				pst.setString(3, rs.getString("dtsid"));
				pst.setString(4, rs.getString("dtstype"));
				pst.setString(5, tableguid);
				pst.setString(6, schemaverguid);
				pst.addBatch();
			}
			pst.executeBatch();
			close(pst);
		} catch (SQLException e) {
			logger.error( "{}",e);
			throw e;
		}		
	}
	
	
	
	public ResultSet getrResultSetBySQL(String sql , Connection conn) throws SQLException {
		logger.info(sql);
		return conn.createStatement().executeQuery(sql);
	}
	
	public int upDateBySQL(String sql, Connection conn) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		}finally{
			close(stmt);
		}
		return 0;
	}
	
	public void close(ResultSet rs) {
		try {
			if(null != rs){
				if(!rs.isClosed()){
					Statement stmt = rs.getStatement();
					rs.close();
					rs = null;
					close(stmt);
				}
			}
		} catch (Exception e) {
		}
	}
	
	public void close(Statement stmt) {
		try {
			if(null != stmt){
				if(!stmt.isClosed()){
					stmt.close();
					stmt = null;
				}
			}
		} catch (Exception e) {
		}
	}
}
