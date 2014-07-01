package com.victop.tool.dataarchitecture.convert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.victop.tool.dataarchitecture.Convert;

/**
 * Sql转换
 * @author chenzr
 *
 */
public class SqlConvert extends Convert {


	@Override
	public String execute(Map<String, String> params, String resourceid ,
			String resourceguid, String schemaverguid,Map<String, Connection> connMap)
			throws Exception {
		String cdasql = "";
		ResultSet rs = null;
		try {
			cdasql = params.get("cdasql");
			if(null != cdasql && !"".equalsIgnoreCase(cdasql) && !"null".equalsIgnoreCase(cdasql)){
//				logger.info("URL "+ H2Conn.getMetaData().getURL());
				//删除数据
				deleteData(params,resourceid);
				String tableName = params.get("tablename");
				cdasql = cdasql.replaceAll("&resourceguid&", resourceguid);
				cdasql = cdasql.replaceAll("&schemaverguid&", schemaverguid);
				rs = getResultSetBySql(BizConn, cdasql);
				insertDataToTable(tableName, rs);
			}
		} catch (Exception e) {
			throw e;
		}finally{
			closeResultSet(rs);
		}
		return null;
	}
	
	/**
	 * 插入资源到内存表
	 * @param rSet
	 * @throws SQLException 
	 */
	private void insertDataToTable(String tableName, ResultSet rs) throws SQLException {
		StringBuilder fieldStr = new StringBuilder();
		StringBuilder placeStr = new StringBuilder();
		ResultSetMetaData rsmd = rs.getMetaData();
		ArrayList<String> fields = new ArrayList<String>();
		Map<String, String> fieldMap = getTableType(tableName, H2Conn);
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			if("SCHEMAVERGUID".equalsIgnoreCase(rsmd.getColumnLabel(i))){
				continue;
			}
			fields.add(rsmd.getColumnLabel(i).toLowerCase());
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
		PreparedStatement pst = H2Conn.prepareStatement(sql);
		while (rs.next()) {
			for (int i = 0; i < fields.size(); i++) {
				String typeStr = fieldMap.get(fields.get(i));
				if("INT".equalsIgnoreCase(typeStr) || "INTEGER".equalsIgnoreCase(typeStr)){
					String string = rs.getString(fields.get(i));
					if(null == string || "".equalsIgnoreCase(string) || "null".equalsIgnoreCase(string)){
						pst.setObject(i+1, null);
					}else{
						pst.setObject(i+1, rs.getObject(fields.get(i)));
					}
				}else{
					pst.setObject(i+1, rs.getObject(fields.get(i)));
				}
			}
			pst.addBatch();
		}
		int[] msg = pst.executeBatch();
		logger.info("{}",msg);
		pst.close();
	}
	
	/**
	 * 返回表字段名类型
	 * @param tableName
	 * @param conn
	 * @return
	 * @throws Exception
	 */
	private Map<String, String> getTableType(String tableName, Connection conn) throws SQLException{
		ResultSet rs = null;
		Statement statement = null;
		Map<String, String> columns = new HashMap<String, String>();
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery("select * from "+tableName + " WHERE 1=2");
			ResultSetMetaData rsm = rs.getMetaData();
			for (int i = 0; i < rsm.getColumnCount(); i++) {
				columns.put(rsm.getColumnLabel(i+1).toLowerCase(), rsm.getColumnTypeName(i+1));
			}
		} catch (SQLException e) {
			logger.error("{}",e);
			throw e;
		}finally{
			if(null != rs){
//				if(!rs.isClosed()){
					rs.close();
//				}
			}
			if(null != statement){
//				if(!statement.isClosed()){
					statement.close();
//				}
			}
		}
		return columns;
	}
	
	/**
	 * 路据资源类型删除数据
	 * @param params
	 * @param resourceguid
	 * @throws SQLException 
	 */
	private void deleteData(Map<String, String> params,String resourceid) throws SQLException {
		try {		
			String tablename = params.get("tablename");
			String wherestr = params.get("wherestr");
			String sql = "DELETE FROM "+ tablename +" WHERE "+wherestr;
			sql = sql.replaceAll("&key&", resourceid);
			upDateBySql(H2Conn, sql);
		} catch (SQLException e) {
			throw e;
		}
	}

}
