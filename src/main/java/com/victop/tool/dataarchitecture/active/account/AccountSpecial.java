package com.victop.tool.dataarchitecture.active.account;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * 特殊业务账务生效接口
 * @author chenzr
 *
 */
public class AccountSpecial extends Active {

	private Logger log = LoggerFactory.getLogger(AccountSpecial.class);
	

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		String accountsId = resourceid;
		log.info("运行特殊账务：{} ，开始构建数据原表", accountsId);

		String activeclassname = "";
		ResultSet Accountsrs = H2Conn
				.prepareStatement(
						"select activeclassname from gmaccont join gmacconttype on gmaccont.type=gmacconttype.accounttype where accountsid='"
								+ accountsId + "'").executeQuery();
		while (Accountsrs.next()) {
			activeclassname = Accountsrs.getString("activeclassname");
		}
		Accountsrs.close();

		if (!"".equalsIgnoreCase(PublicFunc.reStringByNull(activeclassname))
				&& null != activeclassname) {
			Class BaseClass = Class.forName(activeclassname);
			Object BaseInstanc = BaseClass.newInstance();
			Active iInstance = (Active) BaseInstanc;
			iInstance.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
		}

		return "生效成功";
	}
}
