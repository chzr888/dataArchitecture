package com.victop.tool.dataarchitecture.active.account;

import java.sql.Connection;
import java.util.Map;

import com.victop.tool.dataarchitecture.Active;
import com.victop.tool.dataarchitecture.utils.Utils;

public class UpDateArchitecture extends Active  {

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		try {
			String dtsid = resourceid;
			String schemaverguid = schemaverGuid;
			Utils utils = new Utils();
			utils.insertDts(schemaverguid, dtsid, "明细账", H2Conn, BizConn);
			utils.insertDts(schemaverguid, dtsid, "即时账", H2Conn, BizConn);
			utils.insertDts(schemaverguid, dtsid, "期间账", H2Conn, BizConn);

		} catch (Exception e) {
			throw e;
		}
		return null;
	}

}
