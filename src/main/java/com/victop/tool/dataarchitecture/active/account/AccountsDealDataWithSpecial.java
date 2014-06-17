package com.victop.tool.dataarchitecture.active.account;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * 特殊业务账务生成数据原表"
 * @author chenzr
 *
 */
public class AccountsDealDataWithSpecial extends Active {

	private Logger log = LoggerFactory
			.getLogger(AccountsDealDataWithSpecial.class);

	private String accountsId = "";
	

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		accountsId = resourceid;
		log.info("先进先出账务编号：{} ，开始构建数据原表", accountsId);

		String businesstype = "", sharecode = "", type = "";
		ResultSet Accountsrs = H2Conn.prepareStatement(
				"select * from gmaccont where accountsid='" + accountsId + "'")
				.executeQuery();
		while (Accountsrs.next()) {
			businesstype = Accountsrs.getString("businesstype");
			sharecode = Accountsrs.getString("sharecode");
			type = Accountsrs.getString("type");
		}
		Accountsrs.close();

		String dtsID = accountsId + "先进先出明细账";

		String dtsTableName = "Tact" + System.currentTimeMillis();
		AccountsInsertDts(accountsId, "先进先出明细账", dtsID,
				dtsTableName + "detail", businesstype, sharecode, H2Conn);

		// gmmdldts
		ResultSet gmmdldts = H2Conn.prepareStatement(
				"select PhysicalTable from gmmdldts where dtsid='" + dtsID
						+ "'").executeQuery();
		if (gmmdldts.next()) {
			dtsTableName = gmmdldts.getString("PhysicalTable");
		}
		gmmdldts.close();
		// --生效
		DealDtsData DtsAt = new DealDtsData();
		DtsAt.AccountsDtsActive(dtsID, H2Conn);

		log.info("先进先出账务编号：{} ，数据原表处理完成", accountsId);

		// ---处理语句
		AccountsDeal(accountsId, type, "先进先出明细账", dtsID, dtsTableName,
				businesstype, sharecode, H2Conn);

		return null;
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
			upDateBySql(
					"insert into gmmdldts(dtsid,dtstype,PhysicalTable,businesstype,sharecode)"
							+ "values('" + dtsID + "','" + dtsType + "','"
							+ dtsTableName + "','" + businesstype + "','"
							+ sharecode + "')", metaConn);
		} catch (Exception e) {
		}

		String sqlWhere = "";

		upDateBySql("delete from gmdtsdim where dtsid='" + dtsID
				+ "'", metaConn);
		upDateBySql("delete from gmdtscol where dtsid='" + dtsID
				+ "'", metaConn);

		upDateBySql(
						"insert into gmdtsdim(dtsid,dimno,dimname,dimprop,dimfield)"
								+ "select '"
								+ dtsID
								+ "',1,a.mastername,a.dimprop,b.columnid "
								+ "from gmAccontDim a left outer join gmmastertablecolumn b on  a.mastername=b.mastername and a.dimprop=b.columncaption "
								+ "left join gmmaster c  on b.mastername=c.mastername "
								+ "where a.accountsid='" + accountsid + "'"
								+ sqlWhere, metaConn);

		sqlWhere = "";
		ResultSet col = PublicFunc
				.getResultSetBySql(
						"select columnid,datatype from gmAccontCol a  join gmFiFoSet b on a.accountsid=b.accountsid and a.columnid=b.accountsitem  where  a.accountsid='"
								+ accountsid + "' " + sqlWhere, metaConn);

		int x = 1;
		String feild = "";
		while (col.next()) {
			feild = "field" + Integer.toString(x);
			upDateBySql(
					"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
							+ "values ('" + dtsID + "','"
							+ col.getString("columnid") + "','" + feild + "','"
							+ col.getString("datatype") + "')", metaConn);
			x = x + 1;
		}
		col.close();

	}

	/**
	 * 先进先出处理语句,暂时默认单据日期作为入库时间
	 * 
	 * @param accountsid
	 * @param dtsType
	 * @param dtsID
	 * @param dtsTableName
	 * @param businesstype
	 * @param sharecode
	 * @param metaConn
	 * @throws SQLException
	 */
	private void AccountsDeal(String accountsid, String accountstype,
			String dtsType, String dtsID, String dtsTableName,
			String businesstype, String sharecode, Connection metaConn)
			throws SQLException {

		// ------accountsid查找物理表
		log
				.debug("select * from gmmdldts where dtsid ='" + accountsid
						+ "明细账'");
		ResultSet gmmdldts = metaConn.prepareStatement(
				"select * from gmmdldts where dtsid ='" + accountsid + "明细账'")
				.executeQuery();
		String PhysicalTable = "";
		while (gmmdldts.next()) {
			PhysicalTable = gmmdldts.getString("PhysicalTable");
		}
		gmmdldts.close();

		// --删除所有设置数据
		upDateBySql("delete from  gmFiFoBackSql where accountsId='"
				+ accountsId + "'", metaConn);

		String selectAccountSql = "select * from " + PhysicalTable
				+ " where doccode='&doccode&'"; // /明细账查询数据表

		// -----构建明细项内存表，并且插入相关语句
		log.info("先进先出账务编号：{} ，{}", accountsId, "插入相关语句");

		log
				.debug("select a.columncaption,a.columnid,a.iskey,b."
						+ "h2str as datastr,a.mastername,a.propname,b.propdatafield from gmdtstablecolumn a "
						+ "join gmdatatype b on a.datatype=b.datatype "
						+ "where a.dtsid='" + dtsID + "'");
		ResultSet columnRs = metaConn
				.prepareStatement(
						"select a.columncaption,a.columnid,a.iskey,b."
								+ "h2str as datastr,a.mastername,a.propname,b.propdatafield from gmdtstablecolumn a "
								+ "join gmdatatype b on a.datatype=b.datatype "
								+ "where a.dtsid='" + dtsID + "'")
				.executeQuery();

		String createsql = " CREATE MEMORY LOCAL TEMPORARY TABLE "
				+ dtsTableName + " (&fieldlist&) NOT PERSISTENT", createfield = "";
		Map<String, String> dtatype = new HashMap<String, String>();
		String iskey = "", columnid = "", dccolumnid = "", datastr = "", columnidNUll = "", mastername = "", propdatafield = "", propname = "";
		String creatememresult = " ";
		createfield = " itemname  varchar(200)  ";
		String insertStrTemp = " itemname ", insertvalue = " '&itemname&' ", dcinsertvalue = "'&itemname&'"; // 插入数据
		String whereStr = " itemname='&itemname&' ", upwhereStr = "";
		while (columnRs.next()) {
			columnid = columnRs.getString("columnid").toLowerCase();
			dccolumnid = columnid;
			iskey = columnRs.getString("iskey");
			datastr = columnRs.getString("datastr");
			mastername = PublicFunc.reStringByNull(columnRs
					.getString("mastername"));
			propdatafield = PublicFunc.reStringByNull(columnRs
					.getString("propdatafield"));
			propname = PublicFunc
					.reStringByNull(columnRs.getString("propname"));

			if ("".equals(mastername)) {
				if (!"".equals(PublicFunc.reStringByNull(dtatype
						.get(propdatafield)))) {
					continue;
				}
				dtatype.put(propdatafield, "1");
				columnid = propdatafield;
				createfield = createfield + "  ";// ---回填量

				creatememresult = columnid + " " + datastr + "  ";
				dccolumnid = "digit";
			}

			if ("先进先出引用唯一号".equals(propname)) {
				insertvalue = insertvalue + ",'&vp_rowid&' ";
			} else if ("先进先出引用单号".equals(propname)) {
				insertvalue = insertvalue + ",'&doccode&' ";
			} else {
				insertvalue = insertvalue + ",'&" + columnid + "&' ";
			}

			createfield = createfield + "," + columnid + " " + datastr + " ";
			insertStrTemp = insertStrTemp + "," + columnid;
			dcinsertvalue = dcinsertvalue + ",'&" + dccolumnid + "&' ";
			if ("1".equals(iskey)) {
				if (!"fiforefrowid".equalsIgnoreCase(columnid)) {
					whereStr = whereStr + " and " + columnid + "='&" + columnid
							+ "&'";
					if (!"vp_rowid".equalsIgnoreCase(columnid)) {
						upwhereStr = PublicFunc.reConnectString(upwhereStr,
								columnid + "='&" + columnid + "&'", " and ");
					}
				}
			}
		}
		columnRs.close();

		createsql = createsql.replaceAll("&fieldlist&", createfield);
		String selectSql = "select * from " + dtsTableName;
		String deleteSql = "delete from " + dtsTableName;
		String dropsql = "drop table " + dtsTableName;

		upDateBySql(
				"insert into gmFiFoBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','selectaccountSql','"
						+ selectAccountSql.replaceAll("'", "''") + "')",
				metaConn);

//		upDateBySql(
//				"insert into gmFiFoBackSql(accountsId,type,sqlstr) values('"
//						+ accountsId + "','creatememsql','" + createsql + "')",
//				metaConn);
		upDateBySql(
				"insert into gmFiFoBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','selectmemsql','" + selectSql + "')",
				metaConn);
		upDateBySql(
				"insert into gmFiFoBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','deletememsql','" + deleteSql + "')",
				metaConn);
		upDateBySql(
				"insert into gmFiFoBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','dropmemsql','" + dropsql + "')",
				metaConn);

		String creatememStr = "CREATE MEMORY LOCAL TEMPORARY TABLE "
				+ "tempresult (" + creatememresult
				+ ",backfilling float ) NOT PERSISTENT";
		upDateBySql(
				"insert into gmFiFoBackSql (accountsId,type,sqlstr) values('"
						+ accountsId + "','createtempresult','"
						+ creatememStr.replaceAll("'", "''") + "')", metaConn);
		creatememStr = "drop  TABLE " + " tempresult ";
		upDateBySql(
				"insert into gmFiFoBackSql (accountsId,type,sqlstr) values('"
						+ accountsId + "','droptempresult','"
						+ creatememStr.replaceAll("'", "''") + "')", metaConn);

		// -----------插入数据和分拆查询数据sql

		String sqlStr = "select * from gmFiFoSet where accountsid='"
				+ accountsId + "'";

		ResultSet rs = metaConn.prepareStatement(sqlStr).executeQuery();
		String accountsitem = "", direction = "", insertStr = "", updateStr = "", selectStr = "";
		while (rs.next()) {
			updateStr = "";
			selectStr = "";
			accountsitem = rs.getString("accountsitem");
			direction = rs.getString("direction");
			insertStr = "insert into "
					+ dtsTableName
					+ "("
					+ insertStrTemp
					+ ") values("
					+ insertvalue.replaceAll("'&backfilling&'", "0")
							.replaceAll("'&breaktime&'", "'&docdate&'")
							.replaceAll("'&direction&'", "'" + direction + "'")
					+ ")";

			

			// ---分拆查询
			if ("减".equals(direction)) {
				insertStr = "insert into "
						+ dtsTableName
						+ "("
						+ insertStrTemp
						+ ") values("
						+ dcinsertvalue.replaceAll("'&backfilling&'", "0")
								.replaceAll("'&breaktime&'", "'&docdate&'")
								.replaceAll("'&direction&'",
										"'" + direction + "'")+")";

				if ("先进先出".equals(accountstype)) {
					selectStr = "select * from (select &top& *  from "
							+ dtsTableName
							+ "  where   direction='加' and (floatvalue-backfilling>0) 	and  "
							+ upwhereStr
							+ "     order by breaktime  asc   &limit&  ) &rownum& ";
				} else {
					selectStr = "select * from (select &top& *  from "
							+ dtsTableName
							+ "  where   direction='加' and (floatvalue-backfilling>0) 	and  "
							+ upwhereStr
							+ "   order by breaktime  desc   &limit& ) &rownum& ";
				}

				// --更新查询

				updateStr = "update "
						+ dtsTableName
						+ " set backfilling=backfilling+'&backfilling&' "
						+ " where "
						+ whereStr
						+ "  and direction='加' and (floatvalue-backfilling-'&backfilling&')>=0  ";

			}

			upDateBySql(
					"update  gmFiFoSet set selectSql='"
							+ selectStr.replaceAll("'", "''")
							+ "' where accountsId='" + accountsId
							+ "' and accountsitem='" + accountsitem + "' ",
					metaConn);

			upDateBySql(
					"update  gmFiFoSet set updateSql='"
							+ updateStr.replaceAll("'", "''")
							+ "' where accountsId='" + accountsId
							+ "' and accountsitem='" + accountsitem + "' ",
					metaConn);
			
			// ---插入数据
			upDateBySql(
					"update  gmFiFoSet set insertSql='"
							+ insertStr.replaceAll("'", "''")
							+ "'  where accountsId='" + accountsId
							+ "' and accountsitem='" + accountsitem + "' ",
					metaConn);
		}
	}
	
	
	/**
	 * 更新数据
	 * 
	 * @param conn
	 * @param sql
	 * @throws SQLException
	 */
	public void upDateBySql( String sql,Connection conn) throws SQLException {
		Statement stmt = null;
		try {
			logger.info(sql);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		} finally {
			if (null != stmt) {
				if (!stmt.isClosed()) {
					stmt.close();
					stmt = null;
				}
			}
		}
	}


	/**
	 * 查询数据
	 * 
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getResultSetBySql( String sql ,Connection conn)
			throws SQLException {
		logger.info(sql);
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}


}
