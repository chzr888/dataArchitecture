package com.victop.tool.dataarchitecture.active.bsrelation;

import java.sql.Connection;
import java.util.Map;

import com.victop.tool.dataarchitecture.Active;
import com.victop.tool.dataarchitecture.utils.Utils;

public class BsRelationUpDate extends Active {

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		try {
			String dtsid = resourceid;
			Utils utils = new Utils();
			utils.insertDts(schemaverGuid, dtsid, "业务关联", H2Conn, BizConn);
			utils.insertDts(schemaverGuid, dtsid+ "_关系", "业务关联", H2Conn, BizConn);

		} catch (Exception e) {
			throw e;
		}
		return null;
	}

}
