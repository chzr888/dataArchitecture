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
 * 业务账务预处理
 * 
 * @author znyuan
 * 
 */
public class AccountsCreateSqlWithNormal extends Active {

	private Logger log = LoggerFactory
			.getLogger(AccountsCreateSqlWithNormal.class);

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		String accountsId = resourceid;

		String dtsID = accountsId + "明细账", dtsTimeID = accountsId + "即时账", dtsPeriodID = accountsId
				+ "期间账";
		log.info("账务编号：{} ，预处理开始", accountsId);

		this.upDateBySql(
				"delete from gmAccountBackSql where accountsId='" + accountsId
						+ "'", H2Conn);

		ResultSet gmmdldts = H2Conn.prepareStatement(
				"select * from gmmdldts where dtsid in('" + dtsID + "','"
						+ dtsTimeID + "','" + dtsPeriodID + "')")
				.executeQuery();

		String PhysicalTable = "", timeTable = "", periodTable = "";
		while (gmmdldts.next()) {
			if (dtsID.equals(gmmdldts.getString("dtsid"))) {
				PhysicalTable = gmmdldts.getString("PhysicalTable");
			}
			if (dtsTimeID.equals(gmmdldts.getString("dtsid"))) {
				timeTable = gmmdldts.getString("PhysicalTable");
			}
			if (dtsPeriodID.equals(gmmdldts.getString("dtsid"))) {
				periodTable = gmmdldts.getString("PhysicalTable");
			}
		}
		gmmdldts.close();

		// -----构建明细项内存表，并且插入相关语句
		log.info("账务编号：{} ，{}", accountsId, "构建明细项内存表，并且插入相关语句");
		ResultSet columnRs = H2Conn
				.prepareStatement(
						"select a.columnid,a.iskey,b."
								+ "h2str as datastr,a.mastername,b.propdatafield from gmdtstablecolumn a "
								+ "join gmdatatype b on a.datatype=b.datatype "
								+ "where a.dtsid='" + dtsID + "'")
				.executeQuery();

		String createsql = " CREATE MEMORY LOCAL TEMPORARY TABLE "
				+ PhysicalTable + " (&fieldlist&) NOT PERSISTENT", createfield = "";
		Map<String, String> dtatype = new HashMap<String, String>();
		String iskey = "", columnid = "", datastr = "", columnidNUll = "", mastername = "", propdatafield = "";

		createfield = " itemname  varchar(200) ";
		while (columnRs.next()) {
			columnid = columnRs.getString("columnid").toLowerCase();
			iskey = columnRs.getString("iskey");
			datastr = columnRs.getString("datastr");
			mastername = PublicFunc.reStringByNull(columnRs
					.getString("mastername"));
			propdatafield = PublicFunc.reStringByNull(columnRs
					.getString("propdatafield"));

			if ("".equals(mastername)) {
				if (!"".equals(PublicFunc.reStringByNull(dtatype
						.get(propdatafield)))) {
					continue;
				}
				dtatype.put(propdatafield, "1");
				columnid = propdatafield;
			}
			createfield = createfield + "," + columnid + " " + datastr + " ";
		}
		columnRs.close();

		createsql = createsql.replaceAll("&fieldlist&", createfield);
		String selectSql = "select * from " + PhysicalTable;
		String deleteSql = "delete from " + PhysicalTable;
		String dropsql = "drop table " + PhysicalTable;

		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','creatememsql','" + createsql + "')",
						H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','selectmemsql','" + selectSql + "')",
						H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','deletememsql','" + deleteSql + "')",
						H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','dropmemsql','" + dropsql + "')",
						H2Conn);

		// ---插入即时账插入数据
		log.info("账务编号：{} ，{}", accountsId, "插入即时账插入数据");
		ResultSet gmTimeCol = H2Conn
				.prepareStatement(
						"select a.columnid,a.iskey,b.propdatafield,b.notspace,b.h2str from gmDtsTableColumn a join gmdatatype b on a.datatype=b.datatype where a.Dtsid='"
								+ dtsTimeID + "'").executeQuery();
		String fiskey = "", fcolumnid = "", fpropdatafield = "";
		String timeUpdate = "", timeselect = "", timeValue = "", timeWhere = "itemname='&itemname&'", timeInsert = "itemname", timeInsertValue = "'&itemname&'", timeInsertSelect = "itemname", notspace = "";
		createsql = "";
		String InsertTimeSql = "", InsertTimeSelect = "", deleteTimeSql = "", DropTimeSql = "";
		while (gmTimeCol.next()) {
			fcolumnid = gmTimeCol.getString("columnid").toLowerCase();
			fiskey = gmTimeCol.getString("iskey");
			fpropdatafield = gmTimeCol.getString("propdatafield");
			notspace = gmTimeCol.getString("notspace");
			datastr = gmTimeCol.getString("h2str");
			if ("1".equals(fiskey)) {
				if (!"".equals(timeWhere)) {
					timeWhere = timeWhere + " and ";
					timeInsertSelect = timeInsertSelect + ",";
					timeInsertValue = timeInsertValue + ",";
				}
				timeWhere = timeWhere + fcolumnid + "='&" + fcolumnid + "&'";
				timeInsertSelect = timeInsertSelect + fcolumnid;
				timeInsertValue = timeInsertValue + "'&" + fcolumnid + "&'";

				if (!"".equals(createsql)) {
					createsql = createsql + ",";
					InsertTimeSelect = InsertTimeSelect + ",";
					InsertTimeSql = InsertTimeSql + ",";
				}
				createsql = createsql + fcolumnid + " " + datastr + " ";
				InsertTimeSelect = InsertTimeSelect + fcolumnid;
				InsertTimeSql = InsertTimeSql + "'&" + fcolumnid + "&'";
			} else {
				if (!"".equals(createsql)) {
					createsql = createsql + ",";
				}
				createsql = createsql + fcolumnid + " " + datastr
						+ " DEFAULT 0 ";

			}
		}
		gmTimeCol.close();

		// ---插入公式内存数据库
		log.info("账务编号：{} ，{}", accountsId, "插入公式内存数据库");
		createsql = " CREATE MEMORY LOCAL TEMPORARY TABLE " + timeTable + " ("
				+ createsql + ") NOT PERSISTENT";
		selectSql = "select * from " + timeTable;
		InsertTimeSql = " insert into " + timeTable + "(" + InsertTimeSelect
				+ ",&field&)values(" + InsertTimeSql
				+ ",&direction&(&fieldvalue&))";
		deleteSql = "delete from " + timeTable;
		dropsql = "drop table " + timeTable;
		timeUpdate = " update " + timeTable
				+ " set &field&=&field&+(&direction&(&fieldvalue&)) where "
				+ timeWhere.replaceAll("itemname='&itemname&'" + " and ", "");
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timecreatememsql','" + createsql
						+ "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timeselectmemsql','" + selectSql
						+ "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timeinsertmemsql','"
						+ InsertTimeSql.replaceAll("'", "''") + "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timedeletememsql','" + deleteSql
						+ "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timedropmemsql','" + dropsql + "')",
						H2Conn);

		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timeupdatememsql','"
						+ timeUpdate.replaceAll("'", "''") + "')", H2Conn);

		// -----更新即时账物理表
		log.info("账务编号：{} ，{}", accountsId, "更新即时账物理表");
		timeUpdate = "update " + timeTable
				+ " set &field&=&field&+(&direction&(&fieldvalue&)) where "
				+ timeWhere;
		timeselect = "select 1 from " + timeTable + " where " + timeWhere;
		timeInsert = "insert into " + timeTable + "(" + timeInsertSelect
				+ ",&field&)" + "values(" + timeInsertValue
				+ ",&direction&(&fieldvalue&))";

		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timeupdate','"
						+ timeUpdate.replaceAll("'", "''") + "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timeinsert','"
						+ timeInsert.replaceAll("'", "''") + "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','timeselect','"
						+ timeselect.replaceAll("'", "''") + "')", H2Conn);

		// ---插入期间账插入数据
		log.info("账务编号：{} ，{}", accountsId, "插入期间账插入数据");
		ResultSet gmPeriodIdCol = H2Conn
				.prepareStatement(
						"select a.columnid,a.iskey,b.propdatafield,b.notspace from gmDtsTableColumn a join gmdatatype b on a.datatype=b.datatype where a.Dtsid='"
								+ dtsPeriodID + "'").executeQuery();
		fiskey = "";
		fcolumnid = "";
		fpropdatafield = "";
		timeUpdate = "";
		timeValue = "";
		timeWhere = "itemname='&itemname&'";
		timeInsert = "itemname";
		timeInsertValue = "'&itemname&'";
		timeInsertSelect = "itemname";
		notspace = "";
		while (gmPeriodIdCol.next()) {
			fcolumnid = gmPeriodIdCol.getString("columnid").toLowerCase();
			fiskey = gmPeriodIdCol.getString("iskey");
			fpropdatafield = gmPeriodIdCol.getString("propdatafield");
			notspace = gmPeriodIdCol.getString("notspace");
			if ("1".equals(fiskey)) {
				if (!"".equals(timeWhere)) {
					timeWhere = timeWhere + " and ";
					timeInsertSelect = timeInsertSelect + ",";
					timeInsertValue = timeInsertValue + ",";
				}
				timeWhere = timeWhere + fcolumnid + "='&" + fcolumnid + "&'";
				timeInsertSelect = timeInsertSelect + fcolumnid;
				timeInsertValue = timeInsertValue + "'&" + fcolumnid + "&'";
			}
		}
		gmPeriodIdCol.close();

		timeselect = "select 1 from " + periodTable + " where " + timeWhere;

		timeUpdate = "update " + periodTable
				+ " set &field&=&field&+(&fieldvalue&)  where " + timeWhere;
		timeInsert = "insert into " + periodTable + "(" + timeInsertSelect
				+ ",periodin,periodout,periodbegin,periodend)" + "values("
				+ timeInsertValue + ",&periodin&,&periodout&,0,0)";

		String timePeriod = "update " + periodTable
				+ " set periodend=periodbegin+periodin-periodout  where "
				+ timeWhere;

		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','periodselect','"
						+ timeselect.replaceAll("'", "''") + "')", H2Conn);

		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','periodupdate','"
						+ timeUpdate.replaceAll("'", "''") + "')", H2Conn);
		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','periodinsert','"
						+ timeInsert.replaceAll("'", "''") + "')", H2Conn);

		this.upDateBySql(
				"insert into gmAccountBackSql(accountsId,type,sqlstr) values('"
						+ accountsId + "','periodcalc','"
						+ timePeriod.replaceAll("'", "''") + "')", H2Conn);

		
		log.info("账务编号：{} ，{}", accountsId, "整理明细表对应即时账");
		this.upDateBySql(
				"delete from gmAccountBackcol where accountsId='" + accountsId
						+ "'", H2Conn);

		ResultSet gmAccontCol = H2Conn
				.prepareStatement(
						"select  a.columnid,a.direction,a.matchingcol,d.isperiodidcol,d.isNoNeg,b.propdatafield,c.columnid as timefieldname from gmAccontCol a "
								+ " join gmdatatype b  on a.datatype=b.datatype "
								+ " left outer join gmdtstablecolumn c on a.matchingcol=c.columncaption and c.dtsid='"
								+ dtsTimeID
								+ "' left outer join gmAccontCol d on a.accountsId=d.accountsId and a.matchingcol=d.columnid and d.type='即时账'"
								+ " where a.accountsId='"
								+ accountsId
								+ "' and a.type='明细账' ").executeQuery();

		String direction, matchingcol, isperiodidcol, periodfieldName = "", isNoNeg = "", btimeWhere = "", PField = "", timefieldname = "", periodbyzero = "";
		while (gmAccontCol.next()) {
			columnid = gmAccontCol.getString("columnid");
			direction = gmAccontCol.getString("direction");
			btimeWhere = "";
			periodfieldName = "";
			periodbyzero = "";
			timefieldname = gmAccontCol.getString("timefieldname");
			if ("加".equals(direction)) {
				direction = "+";
				PField = "期间入";
			} else if ("减".equals(direction)) {
				direction = "-";
				PField = "期间出";

			}
			matchingcol = gmAccontCol.getString("matchingcol");
			isperiodidcol = PublicFunc.reStringByNull(gmAccontCol
					.getString("isperiodidcol"));
			propdatafield = gmAccontCol.getString("propdatafield");
			isNoNeg = gmAccontCol.getString("isNoNeg");

			if ("1".equals(isNoNeg)) {
				btimeWhere = " and (" + propdatafield + direction
						+ "(&fieldvalue&))>=0 ";
			}

			if ("1".equals(isperiodidcol)) {
				if ("+".equals(direction)) {
					periodfieldName = "periodin";
					periodbyzero = "periodout";
				} else if ("-".equals(direction)) {
					periodfieldName = "periodout";
					periodbyzero = "periodin";
				}
			} else {
				isperiodidcol = "0";
			}

			this.upDateBySql(
							"insert into gmAccountBackcol(AccountsID,detailcol,Direction,fieldname,timecol,timeWhere,isPeriod,timefieldname,periodfieldName,periodbyzero)"
									+ "values('"
									+ accountsId
									+ "','"
									+ columnid
									+ "','"
									+ direction
									+ "','"
									+ propdatafield
									+ "','"
									+ matchingcol
									+ "','"
									+ btimeWhere
									+ "','"
									+ isperiodidcol
									+ "','"
									+ timefieldname
									+ "','"
									+ periodfieldName
									+ "','"
									+ periodbyzero + "')", H2Conn);

		}
		gmAccontCol.close();

		this.upDateBySql(
				"delete  from gmAccountBackformula where accountsId='"
						+ accountsId + "' ", H2Conn);
		// ----公式整理
		log.info("账务编号：{} ，{}", accountsId, "公式整理");
		ResultSet gmTimeAccontCol = H2Conn
				.prepareStatement(
						"select a.columnid,b.columnid as field from gmAccontCol a left outer join gmdtsTableColumn b on a.columnid=b.columncaption and b.dtsid='"
								+ dtsTimeID
								+ "' where a.accountsId='"
								+ accountsId
								+ "' and a.type='即时账' order by LENGTH(a.columnid) desc")
				.executeQuery();

		String formula = "", repaceColumnid = "", field = "";
		ResultSet gmTimeAccontColformula = this.getResultSetBySql(
						"select  a.columnid,a.formula,a.direction,a.matchingcol,a.isperiodidcol,a.isNoNeg,c.propdatafield from gmAccontCol a  "
								+ " join gmdatatype c  on a.datatype=c.datatype"
								+ " left join gmdtsTableColumn b  "
								+ " on  a.columnid=b.columncaption and b.dtsid='"
								+ dtsTimeID
								+ "'  where a.accountsId='"
								+ accountsId
								+ "' and type='即时账' and  (a.formula<>'' and a.formula is not null)",
								H2Conn);
		while (gmTimeAccontColformula.next()) {
			formula = gmTimeAccontColformula.getString("formula");
			columnid = gmTimeAccontColformula.getString("columnid");
			isNoNeg = gmTimeAccontColformula.getString("isNoNeg");
			propdatafield = gmTimeAccontColformula.getString("propdatafield");
			String formulaWhere = "";
//			gmTimeAccontCol.beforeFirst();
			while (gmTimeAccontCol.next()) {
				repaceColumnid = gmTimeAccontCol.getString("columnid");
				field = gmTimeAccontCol.getString("field");
				if (formula.indexOf(repaceColumnid) >= 0) {
					formula = formula.replaceAll(repaceColumnid, "&" + field
							+ "&");
					this.upDateBySql(
							"insert into gmAccountBackformula(accountsID,timecol,detailcol) values('"
									+ accountsId + "','" + columnid + "','"
									+ repaceColumnid + "') ", H2Conn);
				}
			}
			if ("1".equals(isNoNeg)) {
				formulaWhere = ", formulaWhere=' and " + propdatafield
						+ "+(&fieldvalue&)>=0 ' ";
			} else {
				formulaWhere = ",formulaWhere='' ";
			}

			this.upDateBySql("update  gmAccontCol set formulaback='"
					+ formula + "'  " + formulaWhere + " where accountsId='"
					+ accountsId + "' and type='即时账' and columnid='" + columnid
					+ "'  ", H2Conn);
		}
		gmTimeAccontCol.close();

		log.info("账务编号：{} ，预处理完成", accountsId);
		return null;
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
