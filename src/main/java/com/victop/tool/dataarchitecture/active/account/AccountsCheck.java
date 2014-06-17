package com.victop.tool.dataarchitecture.active.account;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**.
 * 业务账务生效检查
 * @author znyuan
 *
 */
public class AccountsCheck extends Active {

	/**.
	 *	日志 记录器
	 */
	private Logger log = LoggerFactory.getLogger(AccountsCheck.class);
	
	private String accountsId = "", accountsType = "";
	
	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {


		accountsId = resourceid;
		if ("".equals(PublicFunc.reStringByNull(accountsId))) {
			throw new Exception("没有找到业务账务编号，前检查!");
		}

		log.info("账务编号：{} ，开始检查", accountsId);
		// --获取账务基础信息
		ResultSet gmAccount = H2Conn.prepareStatement(
				"select * from gmaccont where accountsid='" + accountsId + "'")
				.executeQuery();
		while (gmAccount.next()) {
			accountsType = PublicFunc.reStringByNull(gmAccount
					.getString("type"));
		}
		gmAccount.close();

		// --检查期间角度
		ResultSet dimrs = H2Conn
				.prepareStatement(
						"SELECT count(a.mastername) as periodcount FROM gmAccontDim  a join gmmaster b on a.mastername=b.MasterName AND b.businesstype='期间' "
								+ "where a.accountsid='" + accountsId + "'")
				.executeQuery();
		int pcount = 0;
		while (dimrs.next()) {
			pcount = dimrs.getInt("periodcount");
			break;
		}
		dimrs.close();
		if (pcount > 1) {
			throw new Exception("设置了多个期间角度请检查！");
		}
		if (pcount < 1) {
			throw new Exception("请设置了一个期间角度！");
		}

		// -----检查特殊类型角度设置,必须设置一个顺序
//		if (!"".equals(accountsType) && !"正常".equals(accountsType)) {
//			pcount = 0;
//			ResultSet gmAccontDim = metaConn.prepareStatement(
//					"select count(1) from gmAccontDim  where accountsid='"
//							+ accountsId
//							+ "' and (orderStr<>'' and  not isnull(orderstr))")
//					.executeQuery();
//			while (gmAccontDim.next()) {
//				pcount = gmAccontDim.getInt(1);
//			}
//			gmAccontDim.close();
//			if (pcount < 1) {
//				this.throwErrorMessageAndReturn("请设置一个角度有排序");
//			}
//
//		}

		// ----检查明细数据项
		ResultSet colrs = H2Conn
				.prepareStatement(
						"select count(columnid) as colcount  from gmAccontCol where type='明细账' and  accountsid='"
								+ accountsId + "' ").executeQuery();
		pcount = 0;
		while (colrs.next()) {
			pcount = colrs.getInt("colcount");
		}
		colrs.close();
		if (pcount < 1) {
			throw new Exception("请设置一个明细账数据项");
		}

		H2Conn.prepareStatement(
				"update gmaccont set actived=1 where accountsid='" + accountsId
						+ "'").execute();

		log.info("账务编号：{} ，初步检查完毕", accountsId);

		return null;
	}


}
