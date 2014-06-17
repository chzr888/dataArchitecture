package com.victop.tool.dataarchitecture.active.account;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;


public class DealDtsData {

	private Logger log = LoggerFactory.getLogger(DealDtsData.class);
	/**
	 * 数据原表生效
	 * 
	 * @param dtsid
	 * @param metaConn
	 * @throws Exception
	 */
	public void AccountsDtsActive(String dtsid, Connection metaConn)
			throws Exception {
		// --检查主档
		ResultSet gmmdldts = this.getResultSetBySql(
				"select * from gmmdldts where dtsid='" + dtsid + "'", metaConn);

		String reMsgString = DtsActiveCheck(gmmdldts, metaConn);
		if (!"".equals(reMsgString)) {
			throw new Exception(reMsgString);
		}
		gmmdldts.close();
		// 维护后台表数据-------------------------------------------------------------------------------------------
		ResultSet gmmdldts2 = this.getResultSetBySql(
				"select * from gmmdldts where dtsid='" + dtsid + "'", metaConn);
		reMsgString = DtsActiveDealData(gmmdldts2, metaConn);
		if (!"".equals(reMsgString)) {
			throw new Exception(reMsgString);
		}

		gmmdldts2.close();
		// ----更新字段生效
		this.upDateBySql("update gmdtscol set actived=1  where dtsid='"
				+ dtsid + "'", metaConn);
		// ----修改状态
		this.upDateBySql("update gmmdldts set actived=1  where dtsid='"
				+ dtsid + "'", metaConn);
	}

	/**
	 * 检查数据原表设置
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private String DtsActiveCheck(ResultSet gmmdldts, Connection metaConn)
			throws SQLException {
		boolean iscur = false;

		String dtsid = "", physicaltable = "", actived = "", dtstype = "";
//		gmmdldts.beforeFirst();
		while (gmmdldts.next()) {
			dtsid = PublicFunc.reStringByNull(gmmdldts.getString("dtsid"));
			physicaltable = PublicFunc.reStringByNull(gmmdldts
					.getString("physicaltable"));
			actived = PublicFunc.reStringByNull(gmmdldts.getString("actived"));
			dtstype = PublicFunc.reStringByNull(gmmdldts.getString("dtstype"));
			iscur = true;
		}

		if (!iscur) {
			return "请保存数据原表后再生效!";
		}
		// if ("1".equals(actived)) {
		// return "数据原表已经生效!";
		// }
		if ("".equals(physicaltable)) {
			return "请设置数据原表物理存储表名!";
		}

		if ("自定义数据".equalsIgnoreCase(dtstype)
				|| "外部数据".equalsIgnoreCase(dtstype)) {
			return "";
		}

		iscur = false;
		ResultSet gmdtsdim = metaConn.prepareStatement(
				"select * from gmdtsdim where dtsid='" + dtsid + "' ")
				.executeQuery();
		while (gmdtsdim.next()) {
			iscur = true;
			break;
		}
		gmdtsdim.close();

		if (!iscur) {
			// return "该数据原表没有设置角度，请检查!";
		}

		return "";
	}

	// ---整理数据原表后台数据
	private String DtsActiveDealData(ResultSet gmmdldts, Connection metaConn)
			throws SQLException {

		String dtsid = "", periodtype = "", dtstype = "", physicaltable = "", selectStr = "", sqlStr = "", keyStr = "";
//		gmmdldts.beforeFirst();
		while (gmmdldts.next()) {
			dtsid = gmmdldts.getString("dtsid");
			physicaltable = gmmdldts.getString("physicaltable");
			periodtype = PublicFunc.reStringByNull(gmmdldts
					.getString("periodtype"));
			dtstype = gmmdldts.getString("dtstype");
		}
		List<String> sqlList = new ArrayList<String>();

		if ("自定义数据".equalsIgnoreCase(dtstype)
				|| "外部数据".equalsIgnoreCase(dtstype)) {
			ResultSet myDsResultSet = metaConn.prepareStatement(
					"select * from gmdtstablecolumn where dtsid='" + dtsid
							+ "'").executeQuery();

			while (myDsResultSet.next()) {
				selectStr = PublicFunc.reConnectString(selectStr, myDsResultSet
						.getString("columnid"), ",");

				// --关键字
				if ("1".equalsIgnoreCase(myDsResultSet.getString("iskey"))) {
					keyStr = PublicFunc.reConnectString(keyStr, myDsResultSet
							.getString("columnid"), ";");
				}
			}
			myDsResultSet.close();

			if ("".equals(keyStr)) {
				return " 数据原表没有关键字段，请设置";
			}
			if ("".equals(selectStr)) {
				return "数据原表没有设置字段，请设置";
			}

			// ---构建sql语句 sql92
			sqlList.add("delete from gmdtssql where dtsid='" + dtsid + "'");
			sqlStr = "select " + selectStr + " from  " + physicaltable + " ";
			sqlList
					.add("insert into gmdtssql(dtsid,databasetype,sqlstr,isnew)values('"
							+ dtsid + "','sql92','" + sqlStr + "',0)");

			// --数据字典
			sqlList.add("delete from gmmdlentity where entityid='"
					+ physicaltable + "'");
			sqlList
					.add("insert into gmmdlentity(entityid,entitycaption,keyfields)values('"
							+ physicaltable
							+ "','"
							+ dtsid
							+ "','"
							+ keyStr
							+ "')");

			for (int i = 0; i < sqlList.size(); i++) {
				this.upDateBySql(sqlList.get(i), metaConn);
			}
			return "";
		}

		// ---系统设置
		this.upDateBySql("delete from gmdtsfollowprop where dtsid='"
				+ dtsid + "' and dimname in(  select mastername "
				+ "from  GMDTStypeMaster  " + "where  dtstype='" + dtstype
				+ "')", metaConn);

		this.upDateBySql("delete from gmdtsdim where dtsid='" + dtsid
				+ "' and dimname in(  select mastername "
				+ "from  GMDTStypeMaster  " + "where  dtstype='" + dtstype
				+ "')", metaConn);

		this
				.upDateBySql(
						"insert into gmdtsdim(dtsid,dimno,dimname,dimprop,dimfield)"
								+ "select '"
								+ dtsid
								+ "','-1',b.mastername,b.masterprop,c.propfield  "
								+ "from  GMDTStypeMaster a join gmmaster b on a.mastername=b.mastername "
								+ " join gmprop c on b.masterprop=c.propname "
								+ "where  a.dtstype='" + dtstype + "'",
						metaConn);

		this
				.upDateBySql(
						"insert into gmdtsfollowprop(Dtsid,dimname,propname,propfield)"
								+ "select '"
								+ dtsid
								+ "',b.mastername,b.propname,b.propfield  "
								+ "from  GMDTStypeMaster a join gmmasterprop b on a.mastername=b.mastername "
								+ "where  a.dtstype='" + dtstype + "'",
						metaConn);

		// --删除之前设置
		sqlList.add("delete from gmdtstablecolumn where dtsid='" + dtsid + "'");

		// --角度
		ResultSet gmdtsdim = metaConn
				.prepareStatement(
						"select b.masterprop,a.dimfield,c.datatype,b.mastername,b.masterprop as propname from gmdtsdim a "
								+ "join gmmaster b on a.dimname=b.mastername "
								+ "join gmprop c on b.masterprop=c.propname "
								+ "where a.dtsid='" + dtsid + "'")
				.executeQuery();
		while (gmdtsdim.next()) {
			sqlList.add(InsertSqlByDtsTableColumn(dtsid, gmdtsdim
					.getString("masterprop"), gmdtsdim.getString("dimfield"),
					gmdtsdim.getString("datatype"), "0", "1", PublicFunc
							.reStringByNull(gmdtsdim.getString("mastername")),
							PublicFunc.reStringByNull(gmdtsdim.getString("propname"))));

			selectStr = PublicFunc.reConnectString(selectStr, gmdtsdim
					.getString("dimfield"), ",");
			keyStr = PublicFunc.reConnectString(keyStr, gmdtsdim
					.getString("dimfield"), ";");
		}
		gmdtsdim.close();

		// // --期间
		// if (!"".equals(periodtype)) {
		// sqlList.add(InsertSqlByDtsTableColumn(dtsid,
		// gmdtsdim.getString("masterprop"), periodtype, "字符", "-1",
		// "1", "", ""));
		// keyStr = this.reConnectString(keyStr, "periodid", ",");
		// }

		// --跟随属性
		ResultSet gmdtsfollowprop = metaConn
				.prepareStatement(
						"select a.propname,a.propfield,b.datatype,d.mastername,d.columncaption as qpropname "
								+ "from gmdtsfollowprop a  "
								+ "join gmprop b on a.propname=b.propname "
								+ "left join gmmastertablecolumn d on a.dimname=d.mastername and a.propname=d.columncaption  "
								+ "where a.dtsid='" + dtsid + "'")
				.executeQuery();
		while (gmdtsfollowprop.next()) {
			sqlList.add(InsertSqlByDtsTableColumn(dtsid, gmdtsfollowprop
					.getString("propname"), gmdtsfollowprop
					.getString("propfield"), gmdtsfollowprop
					.getString("datatype"), "0", "0", PublicFunc
					.reStringByNull(gmdtsfollowprop.getString("mastername")),
					PublicFunc.reStringByNull(gmdtsfollowprop
							.getString("qpropname"))));
			selectStr = PublicFunc.reConnectString(selectStr, gmdtsfollowprop
					.getString("propfield"), ",");
		}
		gmdtsfollowprop.close();

		// --跟随属性
		ResultSet gmdtscol = metaConn.prepareStatement(
				"select columnID,ColumnField,ColumnType from gmdtscol "
						+ "where dtsid='" + dtsid + "'").executeQuery();
		while (gmdtscol.next()) {
			sqlList.add(InsertSqlByDtsTableColumn(dtsid, gmdtscol
					.getString("columnID"), gmdtscol.getString("ColumnField"),
					gmdtscol.getString("ColumnType"), "0", "0", "", ""));
			selectStr = PublicFunc.reConnectString(selectStr, gmdtscol
					.getString("ColumnField"), ",");
		}
		gmdtscol.close();

		// ---构建sql语句 sql92
		sqlList.add("delete from gmdtssql where dtsid='" + dtsid + "'");
		sqlStr = "select " + selectStr + " from  " + physicaltable + " ";
		sqlList
				.add("insert into gmdtssql(dtsid,databasetype,sqlstr,isnew)values('"
						+ dtsid + "','sql92','" + sqlStr + "',0)");

		// --数据字典
		sqlList.add("delete from gmmdlentity where entityid='" + physicaltable
				+ "'");
		sqlList
				.add("insert into gmmdlentity(entityid,entitycaption,keyfields)values('"
						+ physicaltable + "','" + dtsid + "','" + keyStr + "')");

		for (int i = 0; i < sqlList.size(); i++) {
			upDateBySql(sqlList.get(i), metaConn);
		}

		return "";
	}

	/**
	 * 插入后台表
	 * 
	 * @param DtsID
	 * @param columncaption
	 * @param columnid
	 * @param datatype
	 * @param no
	 * @param iskey
	 * @param mastername
	 * @param propname
	 * @return
	 */
	private String InsertSqlByDtsTableColumn(String DtsID,
			String columncaption, String columnid, String datatype, String no,
			String iskey, String mastername, String propname) {
		String sqlString = "insert into gmdtstablecolumn(DtsID,columncaption,columnid,datatype,no,iskey,mastername,propname) "
				+ "values('"
				+ DtsID
				+ "','"
				+ columncaption
				+ "','"
				+ columnid
				+ "','"
				+ datatype
				+ "','"
				+ no
				+ "','"
				+ iskey
				+ "','"
				+ mastername + "','" + propname + "')";
		return sqlString;
	}

	/**
	 * 插入后台表
	 * 
	 * @param sqlstr
	 * @return
	 */
	private static String InsertSqlByDtsTableColumn(String sqlstr) {
		String sqlString = "insert into gmdtstablecolumn(DtsID,columncaption,columnid,datatype,no,iskey,mastername,propname) "
				+ sqlstr;
		return sqlString;
	}

	/**
	 * 取消数据原表
	 * 
	 * @param dtsid
	 * @param metaConn
	 * @throws Exception 
	 */
	public void AccountsDtsCancel(String dtsid, Connection metaConn) throws Exception {
		// ---检查数据原表有效性
		ResultSet gmmdldts = metaConn.prepareStatement(
				"select * from gmmdldts where dtsid='" + dtsid + "'")
				.executeQuery();

		boolean findDts = false;
		String dtstype = "", physicaltable = "", actived = "";
		while (gmmdldts.next()) {
			dtstype = gmmdldts.getString("dtstype");
			physicaltable = gmmdldts.getString("physicaltable");
			actived = PublicFunc.reStringByNull(gmmdldts.getString("actived"));
			findDts = true;
		}
		if (!findDts) {
			throw new Exception("找不到数据原表：" + dtsid);
		}

		String reMsgString = DtsCancelCheck(actived, metaConn);
		if (!"".equals(reMsgString)) {
			throw new Exception(reMsgString);
		}

		// 维护后台表数据-------------------------------------------------------------------------------------------
		if (!"自定义数据".equalsIgnoreCase(dtstype)
				&& !"外部数据".equalsIgnoreCase(dtstype)) {
			// --系统角度
			this.upDateBySql("delete from gmdtsfollowprop where dtsid='"
					+ dtsid + "' and dimname in(  select mastername "
					+ "from  GMDTStypeMaster  " + "where  dtstype='" + dtstype
					+ "')", metaConn);

			this.upDateBySql("delete from gmdtsdim where dtsid='" + dtsid
					+ "' and dimname in(  select mastername "
					+ "from  GMDTStypeMaster  " + "where  dtstype='" + dtstype
					+ "')", metaConn);

			this
					.upDateBySql("delete from gmdtstablecolumn  where dtsid='"
							+ dtsid + "'", metaConn);
		}

		this.upDateBySql("delete from gmdtssql  where dtsid='" + dtsid
				+ "'", metaConn);

		this.upDateBySql("delete from gmmdlentity  where entityid='"
				+ dtsid + "'", metaConn);

		// ---列
		this.upDateBySql("update gmdtscol set actived=0  where dtsid='"
				+ dtsid + "'", metaConn);
		// ----修改状态
		this.upDateBySql("update gmmdldts set actived=0  where dtsid='"
				+ dtsid + "'", metaConn);

	}

	/**
	 * 取消数据原表生效
	 * 
	 * @param gmmdldts
	 * @param metaConn
	 * @param msgtype
	 * @return
	 * @throws SQLException
	 */
	private String DtsCancelCheck(String actived, Connection metaConn)
			throws SQLException {

		if (!PublicFunc.rebooleanByStr(actived)) {
			return "数据原表已经取消生效!";
		}
		return "";
	}
	
	private ResultSet getResultSetBySql(String sql, Connection conn)
			throws SQLException {
		log.debug(sql);
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}

	private void upDateBySql(String sql, Connection conn) throws SQLException {
		log.info(sql);
		PreparedStatement preparedStatement = conn.prepareStatement(sql);
		preparedStatement.execute();
		preparedStatement.close();
	}
}
