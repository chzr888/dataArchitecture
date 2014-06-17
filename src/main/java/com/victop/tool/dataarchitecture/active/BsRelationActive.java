package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.util.Map;

import com.victop.tool.dataarchitecture.Active;
import com.victop.tool.dataarchitecture.active.bsrelation.BsRelationActived;
import com.victop.tool.dataarchitecture.active.bsrelation.BsRelationDtsActived;
import com.victop.tool.dataarchitecture.active.bsrelation.BsRelationUpDate;

/**
 * 业务关联生效
 */
public class BsRelationActive extends Active {

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		try {
			BsRelationActived actived1 = new BsRelationActived();
			actived1.setConn(H2Conn, BizConn, ConfConn, memConn);
			actived1.execute(params, resourceid, resourceGuid, schemaverGuid,
					connMap);

			BsRelationDtsActived actived2 = new BsRelationDtsActived();
			actived2.setConn(H2Conn, BizConn, ConfConn, memConn);
			actived2.execute(params, resourceid, resourceGuid, schemaverGuid,
					connMap);
			
			BsRelationUpDate actived3 = new BsRelationUpDate();
			actived3.setConn(H2Conn, BizConn, ConfConn, memConn);
			actived3.execute(params, resourceid, resourceGuid, schemaverGuid,
					connMap);
		} catch (Exception e) {
			throw e;
		}
		return null;
	}

}
