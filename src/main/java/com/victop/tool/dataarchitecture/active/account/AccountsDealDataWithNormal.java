package com.victop.tool.dataarchitecture.active.account;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;

import com.victop.tool.dataarchitecture.Active;

/**
 * 业务账务生成数据原表
 * @author znyuan
 *
 */
public class AccountsDealDataWithNormal extends Active {

	private Logger log = LoggerFactory
			.getLogger(AccountsDealDataWithNormal.class);
	private String accountsId = "";


	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		accountsId = resourceid;

		log.info("账务编号：{} ，开始构建数据原表", accountsId);
		String businesstype = "", sharecode = "",tablename="";
		ResultSet Accountsrs = H2Conn.prepareStatement(
				"select * from gmaccont where accountsid='" + accountsId + "'")
				.executeQuery();
		while (Accountsrs.next()) {
			businesstype = Accountsrs.getString("businesstype");
			sharecode = Accountsrs.getString("sharecode");
			//tablename=Accountsrs.getString("tablename");
		}
		Accountsrs.close();

		String dtsID = accountsId + "明细账", dtsTimeID = accountsId + "即时账", dtsPeriodID = accountsId
				+ "期间账";

		String dtsTableName ="" ;
		if(null==tablename||tablename.isEmpty()){
			dtsTableName="Tact" + System.currentTimeMillis();
		}else{
			dtsTableName=tablename;
		}
		// ---插入数据原表中
		AccountsInsertDts(accountsId, "明细账", dtsID, dtsTableName + "detail",
				businesstype, sharecode, H2Conn);
		AccountsInsertDts(accountsId, "即时账", dtsTimeID, dtsTableName + "time",
				businesstype, sharecode, H2Conn);
		AccountsInsertDts(accountsId, "期间账", dtsPeriodID, dtsTableName
				+ "period", businesstype, sharecode, H2Conn);

		log.info("账务编号：{} ，开始对数据原表生效", accountsId);
		// --生效
		DealDtsData DtsAt = new DealDtsData();

		DtsAt.AccountsDtsActive(dtsID, H2Conn);
		DtsAt.AccountsDtsActive(dtsTimeID, H2Conn);
		DtsAt.AccountsDtsActive(dtsPeriodID, H2Conn);

		
		log.info("账务编号：{} ，数据原表处理完成", accountsId);
		return "";
	}

	/**
	 * 插入数据原表
	 * 
	 * @param accountsid
	 * @param dtsType
	 * @param dtsID
	 * @param metaConn
	 * @throws SQLException
	 */
	private void AccountsInsertDts(String accountsid, String dtsType,
			String dtsID, String dtsTableName, String businesstype,
			String sharecode, Connection metaConn) throws SQLException {

		try {
			this.upDateBySql(
					"insert into gmmdldts(dtsid,dtstype,PhysicalTable,businesstype,sharecode)"
							+ "values('" + dtsID + "','" + dtsType + "','"
							+ dtsTableName + "','" + businesstype + "','"
							+ sharecode + "')", metaConn);
		} catch (Exception e) {
		}

		String sqlWhere = "";
		if ("即时账".equals(dtsType)) {
			sqlWhere = " and  c.businesstype<>'期间' ";
		}

		this.upDateBySql("delete from gmdtsdim where dtsid='" + dtsID
				+ "'", metaConn);
		this.upDateBySql("delete from gmdtscol where dtsid='" + dtsID
				+ "'", metaConn);

		this.upDateBySql(
						"insert into gmdtsdim(dtsid,dimno,dimname,dimprop,dimfield)"
								+ "select '"
								+ dtsID
								+ "',1,a.mastername,a.dimprop,b.columnid "
								+ "from gmAccontDim a left outer join gmmastertablecolumn b on  a.mastername=b.mastername and a.dimprop=b.columncaption "
								+ "left join gmmaster c  on b.mastername=c.mastername "
								+ "where a.accountsid='" + accountsid + "'"
								+ sqlWhere, metaConn);

		sqlWhere = "";
		if ("期间账".equals(dtsType)) {
			sqlWhere = " and  a.type='即时账' and a.isPeriodidCol='1' ";
		} else {
			sqlWhere = " and  a.type='" + dtsType + "' ";
		}

		ResultSet col = PublicFunc.getResultSetBySql(
				"select columnid,datatype from gmAccontCol a  where  a.accountsid='"
						+ accountsid + "' " + sqlWhere, metaConn);

		int x = 1;
		String feild = "";
		while (col.next()) {
			if ("期间账".equals(dtsType)) {
				feild = "periodin";
				this.upDateBySql(
						"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
								+ "values ('" + dtsID + "','本期进','" + feild
								+ "','" + col.getString("datatype") + "')",
						metaConn);
				feild = "periodout";
				this.upDateBySql(
						"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
								+ "values ('" + dtsID + "','本期出','" + feild
								+ "','" + col.getString("datatype") + "')",
						metaConn);
				feild = "periodbegin";
				this.upDateBySql(
						"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
								+ "values ('" + dtsID + "','期初','" + feild
								+ "','" + col.getString("datatype") + "')",
						metaConn);
				feild = "periodend";
				this.upDateBySql(
						"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
								+ "values ('" + dtsID + "','期未','" + feild
								+ "','" + col.getString("datatype") + "')",
						metaConn);
				feild = "itemname";
				this.upDateBySql(
						"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
								+ "values ('" + dtsID + "','"
								+ col.getString("columnid") + "','" + feild
								+ "','字符')", metaConn);
				break;
			} else {
				feild = "field" + Integer.toString(x);
				this.upDateBySql(
						"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
								+ "values ('" + dtsID + "','"
								+ col.getString("columnid") + "','" + feild
								+ "','" + col.getString("datatype") + "')",
						metaConn);
				x = x + 1;
			}
		}

	}
	private ResultSet getResultSetBySql(String sql, Connection conn) throws SQLException{
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}
	private void upDateBySql(String sql, Connection conn) throws SQLException{
		Statement stmt = conn.createStatement();
		stmt.execute(sql);
		stmt.close();
	}

}
