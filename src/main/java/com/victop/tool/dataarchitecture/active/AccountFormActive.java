package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.util.Map;

import com.victop.tool.dataarchitecture.Active;
import com.victop.tool.dataarchitecture.active.account.AccountsActiveByform;

/**
 * 功能号对应业务账务生效
 * @author chenzr
 */
public class AccountFormActive extends Active {

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		
		AccountsActiveByform account = new AccountsActiveByform();
		account.setConn(H2Conn, BizConn, ConfConn, memConn);
		account.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
		
		return null;
	}

}
