package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.util.Map;

import com.victop.tool.dataarchitecture.Active;
import com.victop.tool.dataarchitecture.active.account.AccountSpecial;
import com.victop.tool.dataarchitecture.active.account.AccountsCheck;
import com.victop.tool.dataarchitecture.active.account.AccountsCreateSqlWithNormal;
import com.victop.tool.dataarchitecture.active.account.AccountsDealDataWithNormal;
import com.victop.tool.dataarchitecture.active.account.UpDateArchitecture;

/**
 * 业务账务生效
 * @author chenzr
 */
public class AccountActive extends Active {

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		try {
			//业务账务生效检查
			//业务账务生成数据原表
			//业务账务预处理
			//特殊业务账务生效接口

			//1.业务账务生效检查
			AccountsCheck accoun1 = new AccountsCheck();
			accoun1.setConn(H2Conn, BizConn, ConfConn, memConn);
			accoun1.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
			
			//2.业务账务生成数据原表
			AccountsDealDataWithNormal accoun2 = new AccountsDealDataWithNormal();
			accoun2.setConn(H2Conn, BizConn, ConfConn, memConn);
			accoun2.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
			
			//3.业务账务预处理
			AccountsCreateSqlWithNormal accoun3 = new AccountsCreateSqlWithNormal();
			accoun3.setConn(H2Conn, BizConn, ConfConn, memConn);
			accoun3.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
			
			//4.特殊业务账务生效接口
			AccountSpecial accoun4 = new AccountSpecial();
			accoun4.setConn(H2Conn, BizConn, ConfConn, memConn);
			accoun4.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
			
			//5.更新到数据架构
			UpDateArchitecture accoun5 = new UpDateArchitecture();
			accoun5.setConn(H2Conn, BizConn, ConfConn, memConn);
			accoun5.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);

		} catch (Exception e) {
			throw e;
		}
		return null;
	}

}
