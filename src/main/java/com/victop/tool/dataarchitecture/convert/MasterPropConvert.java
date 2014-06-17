package com.victop.tool.dataarchitecture.convert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.victop.tool.dataarchitecture.Convert;

/**
 * 主档主属性
 * 
 * @author chenzr
 * 
 */
public class MasterPropConvert extends Convert {

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		ResultSet rs = null;
		try {
			String sql = "SELECT LogicName,propertyguid FROM Vustableproperty WHERE tabletype = '主档' " +
					" AND iskey = 1 AND schemaverguid = '"+schemaverGuid+"' " +
					" AND tableguid = '"+resourceGuid+"'";
			rs = getResultSetBySql(BizConn, sql);
			if (rs.next()) {
				sql = "";
				PreparedStatement pst = null;
				try {
					//更新主属性
					pst = H2Conn.prepareStatement("UPDATE gmmaster SET MasterProp = ? WHERE MasterName = ?");
					pst.setString(1, rs.getString(1));
					pst.setString(2, resourceid);
					pst.executeUpdate();
//					insertProperty(schemaverGuid, rs.getString(2), rs.getString(1));
				} catch (Exception e) {
					throw e;
				}finally{
					closeStatement(pst);
				}
			}
			closeResultSet(rs);
			
			sql = "SELECT LogicName,propertyguid FROM Vustableproperty WHERE tabletype = '主档' " +
					" AND ismastername = 1 AND schemaverguid = '"+schemaverGuid+"' " +
					" AND tableguid = '"+resourceGuid+"'";
			rs = getResultSetBySql(BizConn, sql);
			if (rs.next()) {
				sql = "";
				PreparedStatement pst = null;
				try {
					//更新主属性名称
					pst = H2Conn.prepareStatement("UPDATE gmmaster SET MasterPropName = ? WHERE MasterName = ?");
					pst.setString(1, rs.getString(1));
					pst.setString(2, resourceid);
					pst.executeUpdate();
//					insertProperty(schemaverGuid, rs.getString(2), rs.getString(1));
				} catch (Exception e) {
					throw e;
				}finally{
					closeStatement(pst);
				}
			}
			closeResultSet(rs);
		} catch (Exception e) {
			throw e;
		}
		return null;
	}
	
	/**
	 * 插入属性
	 * @param schemaverGuid
	 * @param resourceguid
	 * @param resourceid
	 * @return
	 * @throws SQLException
	 */
	private String insertProperty(String schemaverGuid,String resourceguid,String resourceid) throws SQLException {
		String tableName = "T_"+schemaverGuid.replaceAll("-", "");
		upDateBySql(memConn, "DELETE FROM "+tableName+" WHERE Guid='"+resourceguid+"'");
		String sql = "INSERT INTO "+tableName+" (resourceId,Guid,typeId,typeName,schemaverguid) VALUES (?,?,?,?,?)";
		PreparedStatement pst = memConn.prepareStatement(sql);
		pst.setString(1, resourceid);
		pst.setString(2, resourceguid);
		pst.setString(3, "13");
		pst.setString(4, "属性设置");
		pst.setString(5, schemaverGuid);
		int i = pst.executeUpdate();
		logger.info("[{}]",i);
		return null;
	}

}
