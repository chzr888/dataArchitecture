package com.victop.tool.dataarchitecture;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.net.aso.h;

import com.victop.platform.common.util.CheckH2DataBase;
import com.victop.tool.dataarchitecture.convert.SqlConvert;

public class test {
	protected static Logger logger = LoggerFactory.getLogger(test.class);
	
	private static Connection h2Conn = null;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		BuilderHelper helper = new BuilderHelper();
		try {
//			helper.init("344CDC7E-9E74-7D54-86E3-0748C7551CDE");
//			
//			String sql = "SELECT a.tableLogicName AS dtsid, a.LogicName AS columncaption, a.PropertyName AS columnid, a.PropertyType AS datatype, a.iskey AS iskey, a.isMain AS isMain, b.tableLogicName AS MasterName, b.LogicName AS propName, a.listshow AS listshow, a.listlength AS listlength, a.viewplugtype AS viewplugtype, a.notsave AS notsave, a.dateformat AS dateformat, a.defaultvalue AS defaultvalue FROM Vustableproperty a LEFT JOIN Vustableproperty b ON a.maptableguid = b.tableguid AND a.mappropertyguid = b.propertyguid AND b.tabletype = '主档' WHERE a.schemaverguid = '024A8F9D-0184-A103-54EB-D0CA0D7314AC' AND a.tableguid = '0527533A-ACCD-459B-A339-94FC912C3095'";
//			Connection conn = getBizConnection();
//			Statement stmt = conn.createStatement();
//			ResultSet rs = stmt.executeQuery(sql);
//			ResultSetMetaData rsmd = rs.getMetaData();
//			while (rs.next()) {
//				System.out.println();
//				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
//					System.out.print(rs.getString(i) +" ");
//				}
//			}
			
//			helper.h2Conn.setAutoCommit(false);
//			for (int i = 0; i < args.length; i++) {
//				SqlConvert convert = new SqlConvert();
//				convert.setConn(helper.h2Conn, helper.bizConn, helper.confConn, helper.memConn);
////				convert.execute(params, resourceid, resourceguid, schemaverguid, connMap);
//			}
//			helper.h2Conn.commit();
//			String url = "jdbc:h2:D://workspace/convert-data-architecture/024A8F9D-0184-A103-54EB-D0CA0D7314AC";
//			getH2Connection(url);
//			Connection conn = getBizConnection();
//			String sql = "SELECT a.tableLogicName AS dtsid, a.LogicName AS columncaption, a.PropertyName AS columnid, a.PropertyType AS datatype, a.iskey AS iskey, a.isMain AS isMain, b.tableLogicName AS MasterName, b.LogicName AS propName, a.listshow AS listshow, a.listlength AS listlength, a.viewplugtype AS viewplugtype, a.notsave AS notsave, a.dateformat AS dateformat, a.defaultvalue AS defaultvalue FROM Vustableproperty a LEFT JOIN Vustableproperty b ON a.maptableguid = b.tableguid AND a.mappropertyguid = b.propertyguid AND b.tabletype = '主档' WHERE a.schemaverguid = '024A8F9D-0184-A103-54EB-D0CA0D7314AC' AND a.tableguid = '095F11DF-DCA7-485B-A6EC-F0BC22799564'";
////			String sql = "SELECT a.tableLogicName AS dtsid, a.LogicName AS columncaption, a.PropertyName AS columnid, a.PropertyType AS datatype, a.iskey AS iskey, a.isMain AS isMain, b.tableLogicName AS MasterName, b.LogicName AS propName, a.listshow AS listshow, a.listlength AS listlength, a.viewplugtype AS viewplugtype, a.notsave AS notsave, a.dateformat AS dateformat, a.defaultvalue AS defaultvalue FROM Vustableproperty a LEFT JOIN Vustableproperty b ON a.maptableguid = b.tableguid AND a.mappropertyguid = b.propertyguid AND b.tabletype = '主档' WHERE a.schemaverguid = '024A8F9D-0184-A103-54EB-D0CA0D7314AC' AND a.tableguid = '0527533A-ACCD-459B-A339-94FC912C3095'";
//			Statement stmt = conn.createStatement();
//			ResultSet rs = stmt.executeQuery(sql);
////			ResultSetMetaData rsmd = rs.getMetaData();
////			while (rs.next()) {
//				insertDataToTable("gmdtstablecolumn", rs);
////			}
			Class.forName("org.h2.Driver");
			h2Conn = DriverManager.getConnection("jdbc:h2:d://tmp/h2tests2","", "");
			Connection conn = h2Conn;
			CheckH2DataBase checkH2DataBase = new CheckH2DataBase();
			checkH2DataBase.checkDataBase(conn);
			
		} catch (Exception e) {
//			try {
////				helper.h2Conn.rollback();
//			} catch (SQLException e1) {
//				e1.printStackTrace();
//			}
		}
	}
	
	/**
	 * 插入资源到内存表
	 * @param rSet
	 * @throws SQLException 
	 */
	private static void insertDataToTable(String tableName, ResultSet rs) throws SQLException {
		StringBuilder fieldStr = new StringBuilder();
		StringBuilder placeStr = new StringBuilder();
		ResultSetMetaData rsmd = rs.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			if( 0 != fieldStr.length()){
				fieldStr.append(",");
				placeStr.append(",");
			}
			fieldStr.append(rsmd.getColumnLabel(i));
			placeStr.append("?");
		}
		String sql = "INSERT INTO "+tableName+" ("+fieldStr.toString()+")" +
				" VALUES ("+placeStr.toString()+")";
		logger.info(sql);
		PreparedStatement pst = h2Conn.prepareStatement(sql);
		while (rs.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				pst.setObject(i, rs.getObject(i));
			}
			pst.addBatch();
		}
		int[] msg = pst.executeBatch();
		logger.info("{}",msg);
		pst.close();
	}
	
	/**
	 * 返回H2数据库连接
	 * 
	 * @return
	 * @throws Exception
	 */
	private static Connection getH2Connection(String url) throws Exception {
		try {
//			if (null != h2Conn) {
//				return h2Conn;
//			}
			Class.forName("org.h2.Driver");
			h2Conn = DriverManager.getConnection(url,
					"", "");
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
		return h2Conn;
	}


	/**
	 * 返回数据架构数据库连接
	 * 
	 * @return
	 * @throws Exception
	 */
	private static Connection getBizConnection() throws Exception {
		try {
			String url = "jdbc:sqlserver://192.168.25.134;DatabaseName=30vems;user=xmdev;password=xmdev";
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			return DriverManager.getConnection(url);
		} catch (Exception e) {
			throw e;
		}
	}
}
