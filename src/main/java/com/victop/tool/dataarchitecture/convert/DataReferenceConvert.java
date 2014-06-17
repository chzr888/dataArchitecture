package com.victop.tool.dataarchitecture.convert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.victop.tool.dataarchitecture.Convert;

public class DataReferenceConvert extends Convert {

	protected Logger logger = LoggerFactory
			.getLogger(DataReferenceConvert.class);

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		String sql = "SELECT * FROM gmDatareference "
				+ " WHERE SCHEMAVERGUID ='" + schemaverGuid
				+ "' AND modelguid = '" + resourceGuid + "'";
		ResultSet rs = null;
		try {
			// 数据引用参数设置表
			rs = getResultSetBySql(BizConn, sql);
			while (rs.next()) {
				// 更新参数设置(fieldParam)
				ResultSet paramRS = null;
				String datasetid = rs.getString("datasetid");
				String columnid = rs.getString("columnid");
				try {
					String sqlStr = "SELECT * FROM gmDatareferenceparam "
							+ " WHERE SCHEMAVERGUID ='" + schemaverGuid + "'"
							+ " AND modelguid = '" + resourceGuid + "'"
							+ " AND datasetid ='" + datasetid + "'"
							+ " AND columnid = '" + columnid + "'";
					paramRS = getResultSetBySql(BizConn, sqlStr);
					ArrayList<ArrayList<Map<String, String>>> paramList = new ArrayList<ArrayList<Map<String, String>>>();
					ArrayList<Map<String, String>> group = new ArrayList<Map<String, String>>();
					while (paramRS.next()) {
						Map<String, String> map = new HashMap<String, String>();
						// 条件值数据集ID
						map.put("dataid",
								reStringByNull(paramRS.getString("dataid")));
						// 条件字段
						map.put("data",
								reStringByNull(paramRS.getString("data")));
						// 条件字段名称
						map.put("datacaption",
								reStringByNull(paramRS.getString("datacaption")));
						// 匹配逻辑
						map.put("logic",
								reStringByNull(paramRS.getString("logic")));
						// 匹配逻辑
						map.put("logiccaption", reStringByNull(paramRS
								.getString("logiccaption")));
						// 外表字段
						map.put("linkdata",
								reStringByNull(paramRS.getString("linkdata")));
						// 外表字段名称
						map.put("linkdatacaption", reStringByNull(paramRS
								.getString("linkdatacaption")));
						group.add(map);
						
					}
					paramList.add(group);
					String jsonString = JSON.toJSONString(paramList);
					jsonString = jsonString.replaceAll("\"", ";top;");
					String sql2 = "UPDATE gmDatareference SET fieldParam = ? WHERE modelid = ? AND name = ? AND columnid = ?";
					PreparedStatement pst = H2Conn.prepareStatement(sql2);
					pst.setString(1, jsonString);
					pst.setString(2, resourceid);
					pst.setString(3, datasetid);
					pst.setString(4, columnid);
					pst.executeUpdate();
					pst.close();					
				} catch (Exception e) {
					throw e;
				} finally {
					closeResultSet(paramRS);
				}
				
				//数据引用返回设置表（fieldreturn）
				ResultSet fieldReturnRS = null;
				try {
					sql = "SELECT * FROM gmDatareferencereturn " +
						 " WHERE SCHEMAVERGUID ='"+schemaverGuid+"' " +
						 " AND modelguid = '"+resourceGuid+"' " +
						 " AND datasetid ='"+datasetid+"' " +
						 " AND columnid = '"+columnid+"'";
					fieldReturnRS = getResultSetBySql(BizConn, sql);
					ArrayList<Map<String, String>> fieldReturnlist = new ArrayList<Map<String,String>>();
					while (fieldReturnRS.next()) {
						Map<String, String> map = new HashMap<String, String>();
						map.put("data", reStringByNull(fieldReturnRS.getString("data")));
						map.put("datacaption", reStringByNull(fieldReturnRS.getString("datacaption")));
						map.put("linkdata", reStringByNull(fieldReturnRS.getString("linkdata")));
						map.put("linkdatacaption", reStringByNull(fieldReturnRS.getString("linkdatacaption")));
						fieldReturnlist.add(map);
					}
					String jsonString = JSON.toJSONString(fieldReturnlist);
					jsonString = jsonString.replaceAll("\"", ";top;");
					String sql2 = "UPDATE gmDatareference SET fieldreturn = ? WHERE modelid = ? AND name = ? AND columnid = ?";
					PreparedStatement pst = H2Conn.prepareStatement(sql2);
					pst.setString(1, jsonString);
					pst.setString(2, resourceid);
					pst.setString(3, datasetid);
					pst.setString(4, columnid);
					pst.executeUpdate();
					pst.close();		
				} catch (Exception e) {
					throw e;
				}finally{
					closeResultSet(fieldReturnRS);
				}
				
				//数据引用返回默认表（operation）
				ResultSet operationRS = null;
				try {
					sql = "SELECT * FROM gmDatareferenceoperation " +
						 " WHERE SCHEMAVERGUID ='"+schemaverGuid+"' " +
						 " AND modelguid = '"+resourceGuid+"' " +
						 " AND datasetid ='"+datasetid+"' " +
						 " AND columnid = '"+columnid+"'";
					operationRS = getResultSetBySql(BizConn, sql);
					Map<String, Object> operationMap = new HashMap<String, Object>();
					ArrayList<Map<String, String>> list = new ArrayList<Map<String,String>>();
					operationMap.put("operation", "2");
					operationMap.put("isdata", "1");
					while (operationRS.next()) {
						Map<String, String> map = new HashMap<String, String>();
						map.put("iscur", reStringByNull(operationRS.getString("iscur")));
						map.put("columnid", reStringByNull(operationRS.getString("returncolumnid")));
						map.put("columncaption", reStringByNull(operationRS.getString("columncaption")));
						map.put("value", reStringByNull(operationRS.getString("defaultvalue")));
						list.add(map);
					}
					operationMap.put("operation", list);
					String jsonString = JSON.toJSONString(operationMap);
					jsonString = jsonString.replaceAll("\"", ";top;");
					String sql2 = "UPDATE gmDatareference SET operation = ? WHERE modelid = ? AND name = ? AND columnid = ?";
					PreparedStatement pst = H2Conn.prepareStatement(sql2);
					pst.setString(1, jsonString);
					pst.setString(2, resourceid);
					pst.setString(3, datasetid);
					pst.setString(4, columnid);
					pst.executeUpdate();
					pst.close();		
				} catch (Exception e) {
					throw e;
				}finally{
					closeResultSet(operationRS);
				}
			}
			closeResultSet(rs);
		} catch (Exception e) {
			logger.error("{}",e);
			throw e;
		}
		return null;
	}
	
	public String getModelid(){
		return null;
	}

	/**
	 * null字符型返回空值,不是空值则去除前后空格
	 * 
	 * @param data
	 * @return
	 */
	public String reStringByNull(String data) {
		if (data == null) {
			return "";
		} else {
			return data.trim();
		}
	}

}
