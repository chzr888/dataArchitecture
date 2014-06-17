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

public class BusinessSerachConvert extends Convert {

	protected Logger logger = LoggerFactory
			.getLogger(BusinessSerachConvert.class);
	
	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		ResultSet rs = null;
		String tablename = "";
		try {
			String sql = "SELECT bsname,bstype, bstargetguid FROM gmbusinessserach " +
					" WHERE schemaverguid = '"+schemaverGuid+"' AND bsguid = '"+resourceGuid+"'";
			rs = getResultSetBySql(BizConn, sql);
			if (rs.next()) {
				String bsname = rs.getString("bsname");
				String bstype = rs.getString("bstype");
				String bstargetguid = rs.getString("bstargetguid");
				if("主档".equalsIgnoreCase(bstype)){
					ResultSet rs1 = null;
					try {
						String sql1 = "SELECT logicname,tablename FROM Vustablelist " +
								" WHERE schemaverguid = '"+schemaverGuid+"'" +
								" AND tableguid = '"+bstargetguid+"' AND tabletype = '主档'";
						rs1 = getResultSetBySql(BizConn, sql1);
						if(rs1.next()){
							tablename = rs1.getString("tablename");
							upDateBySql(H2Conn, "UPDATE gmbusinessserach SET bstargetid = '"+rs1.getString("logicname")+"'" +
									" WHERE bsname = '"+bsname+"'");
						}
					} catch (Exception e) {
						throw e;
					}finally{
						closeResultSet(rs1);
					}
				}else if("数据集".equalsIgnoreCase(bstype)){
					ResultSet rs2 = null;
					try {
						String sql2 = "SELECT datasetid,sqltablename FROM GMdataset" +
								" WHERE datasetguid = '"+bstargetguid+"' AND schemaverguid = '"+schemaverGuid+"'";
						rs2 = getResultSetBySql(BizConn, sql2);
						if(rs2.next()){
							tablename = rs2.getString("sqltablename");
							upDateBySql(H2Conn, "UPDATE gmbusinessserach SET bstargetid = '"+rs2.getString("datasetid")+"'" +
									" WHERE bsname = '"+bsname+"'");
						}
					} catch (Exception e) {
						throw e;
					}finally{
						closeResultSet(rs2);
					}
				}else{
					ResultSet rs3 = null;
					try {
						String sql3 = "SELECT logicname,tablename FROM Vustablelist " +
								" WHERE schemaverguid = '"+schemaverGuid+"'" +
								" AND tableguid = '"+bstargetguid+"' AND tabletype <> '主档'";
						rs3 = getResultSetBySql(BizConn, sql3);
						if(rs3.next()){
							tablename = rs3.getString("tablename");
							upDateBySql(H2Conn, "UPDATE gmbusinessserach SET bstargetid = '"+rs3.getString("logicname")+"'" +
									" WHERE bsname = '"+bsname+"'");
						}
					} catch (Exception e) {
						throw e;
					}finally{
						closeResultSet(rs3);
					}
				}
			}
			
			//参数转换
			ResultSet rs3 = null;
			try {
				ArrayList<Map<String, String>> list = new ArrayList<Map<String,String>>();
				String sql4 = "SELECT searchid, searchidname, searchname, " +
						" datatype, logic, logicname, viewplugtype, showorder FROM gmbusinessserachparam " +
						" WHERE schemaverguid = '"+schemaverGuid+"' AND bsguid = '"+resourceGuid+"'";
				rs3 = getResultSetBySql(BizConn, sql4);
				while (rs3.next()) {
					Map<String, String> map = new HashMap<String, String>();
					map.put("id", reStringByNull(rs3.getString("showorder")));
					map.put("searchname", reStringByNull(rs3.getString("searchname")));
					map.put("searchid", reStringByNull(rs3.getString("searchid")));
					map.put("searchidname", reStringByNull(rs3.getString("searchidname")));
					map.put("logic", reStringByNull(rs3.getString("logic")));
					map.put("logicname", reStringByNull(rs3.getString("logicname")));
					map.put("datatype", reStringByNull(rs3.getString("datatype")));
					map.put("view", reStringByNull(rs3.getString("viewplugtype")));
					map.put("tablename", tablename);
					list.add(map);
				}
				String jsonStr = JSON.toJSONString(list);
				jsonStr = jsonStr.replaceAll("\"", ";top;");
				PreparedStatement pst = H2Conn.prepareStatement(
						"UPDATE gmbusinessserach SET serachfield = ? WHERE bsname = ?");
				pst.setString(1, jsonStr);
				pst.setString(2, resourceid);
				pst.executeUpdate();
				pst.close();
			} catch (Exception e) {
				throw e;
			}finally{
				closeResultSet(rs3);
			}
			
		} catch (Exception e) {
			logger.error("{}",e);
			throw e;
		}finally{
			closeResultSet(rs);
		}
		
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
