package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.rowset.CachedRowSetImpl;
import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * 数据模型生效"
 * @author chenzr
 *
 */
public class DataModelActive extends Active {

	private Logger log = LoggerFactory.getLogger(DataModelActive.class);
	

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		String modelid = resourceid;
		
		try{

			ResultSet gmmodel = getResultSetBySql(
					"select * from gmmodel where modelid='" + modelid + "'",
					false, H2Conn);
			ResultSet gmmodel_crs =ResultSetToCachedRowSet(gmmodel);
			
			// ---检查暂时M1
			modelActiveCheck(modelid, gmmodel_crs, H2Conn);

			// --M1生效
			modelDealM1(modelid, gmmodel_crs, H2Conn);

			// --批号生效
			modeldealbatch(modelid, H2Conn);

			upDateBySql(
					"update gmmodel set actived='1' where modelid='" + modelid
							+ "' ",false, H2Conn);


		} catch (Exception e) {
			log.error("", e);
			return modelid + ":生效失败【"+e.getMessage()+"】";
		}
		
		return modelid + ":生效成功";
	}

	/**
	 * 数据模型检查
	 * @param modelid
	 *            数据模型ID
	 * @param gmmodel
	 * @param metaConn
	 *            元数据库
	 * @return
	 * @throws SQLException
	 */
	private String modelActiveCheck(String modelid, ResultSet gmmodel,
			Connection metaConn) throws SQLException {
		boolean iscur = false;
		String modeltype = ""; 

		gmmodel.beforeFirst();
		while (gmmodel.next()) {
			modeltype = PublicFunc.reStringByNull(gmmodel
					.getString("modeltype"));
			iscur = true;
		}
		//
		if (!iscur) {
			return "请保存数据模型:" + modelid + ",后在生效!";
		}
		if ("".equals(modeltype)) {
			return "数据模型:" + modelid + "类型为空,请设置后在生效!";
		}

		return "";
	}

	/**
	 * 数据模型M1 生效
	 * @param modelid
	 * @param gmmodel
	 * @param metaConn
	 * @return
	 * @throws Exception
	 */
	private String modelDealM1(String modelid, ResultSet gmmodel,
			Connection metaConn) throws Exception {
		//数据库类型
		String [] databaseType = {"mysql", "mssql", "sql92", "oracle",
				"postgresql", "sybase", "odbc", "db2", "informix", "h2"};
		// --为空函数
		Map<String, String> gmfunctionchange = new HashMap<String, String>();
		gmfunctionchange.put("mssql", "isnull(&field&,'''')=''''");
		gmfunctionchange.put("mysql", "isnull(&field&) or &field&=''''");
		
		boolean iscur = false;
		boolean isDataSet = false;
		List<String> sqlList = new ArrayList<String>();
		String databaseProductName = metaConn.getMetaData().getDatabaseProductName();
		String modeltype = "";
		gmmodel.beforeFirst();
		while (gmmodel.next()) {
			modeltype = gmmodel.getString("modeltype");
		}
		String sqlstrs = "";
		if("SQLite".equalsIgnoreCase(databaseProductName)){
			sqlstrs = "select a.paramid,a.paramcaption,\"a.notnull\",b.notspace "
								+ "from gmmodelparam a join gmdatatype b on  a.datatype=b.datatype "
								+ " where modelid='" + modelid + "'";
		}else{
			sqlstrs = "select a.paramid,a.paramcaption,a.notnull,b.notspace "
								+ "from gmmodelparam a join gmdatatype b on  a.datatype=b.datatype "
								+ " where modelid='" + modelid + "'";
		}
		// --参数
		ResultSet gmmodelparam_rs = getResultSetBySql(sqlstrs,false, metaConn);
		ResultSet gmmodelparam = ResultSetToCachedRowSet(gmmodelparam_rs);
			
		// --构件wheresql语句
		String SqlStr, datasetid, fdatasetid, fdatasetname, masterdatasetid, masterfields, detailfields;
		ResultSet modeldataset_rs = getResultSetBySql("select * "
				+ "from gmmodeldataset where modelid='" + modelid + "'",false,
				metaConn);

		ResultSet modeldataset = ResultSetToCachedRowSet(modeldataset_rs);
		
		// --整理更新字段以及数据库类型
		ResultSetMetaData modeldatasetMD = modeldataset.getMetaData();
		Map<String, Map<String, String>> updateSqlMap = new HashMap<String, Map<String, String>>();


		for (int i = 0; i < databaseType.length; i++) {
			String dbType = databaseType[i];
			for (int j = 0; j < modeldatasetMD.getColumnCount(); j++) {
				String columnid = modeldatasetMD.getColumnLabel(j + 1);
				if (columnid.equalsIgnoreCase(dbType + "wheresql")) {
					Map<String, String> temp = new HashMap<String, String>();
					temp.put("field", columnid);
					temp.put("sqlstr", "");
					updateSqlMap.put(dbType, temp);
				}
			}
		}
		Map<String, Map<String, String>> datasetlist = new HashMap<String, Map<String, String>>();
		while (modeldataset.next()) {
			iscur = true;
			isDataSet = true;
			datasetid = PublicFunc.reStringByNull(modeldataset
					.getString("datasetid"));
			fdatasetid = PublicFunc.reStringByNull(modeldataset
					.getString("fdatasetid"));
			fdatasetname = PublicFunc.reStringByNull(modeldataset
					.getString("fdatasetname"));
			masterdatasetid = PublicFunc.reStringByNull(modeldataset
					.getString("masterdatasetid"));
			masterfields = PublicFunc.reStringByNull(modeldataset
					.getString("masterfields"));
			detailfields = PublicFunc.reStringByNull(modeldataset
					.getString("detailfields"));

			if ("".equals(fdatasetid)) {
				isDataSet = false;
			}
			// 数据集.数据原表
			if (isDataSet) {
				SqlStr = "select a.connectchar,a.leftparse,b.tablename,b.fieldname,logicsymbol,paramcaption,rightparse "
						+ "from gmmodelcdt a left "
						+ "join gmdatastore b on a.fieldcaption=b.fieldcaption "
						+ " where a.modelid='"
						+ modelid
						+ "' and a.datasetid='"
						+ datasetid
						+ "' and b.datasetid='"
						+ fdatasetid
						+ "' order by termid ";
			} else {
				SqlStr = "select a.connectchar,a.leftparse,'' as tablename,b.columnid as fieldname,logicsymbol,paramcaption,rightparse "
						+ " from gmmodelcdt a "
						+ "left join gmdtstablecolumn b on a.fieldcaption=b.columncaption  "
						+ "where a.modelid='"
						+ modelid
						+ "' and a.datasetid='"
						+ datasetid
						+ "' and b.dtsid='"
						+ fdatasetname
						+ "' order by termid ";
			}
			ResultSet gmmodelcdt_rs = getResultSetBySql(SqlStr,false,
					metaConn);
			ResultSet gmmodelcdt = ResultSetToCachedRowSet(gmmodelcdt_rs);
			
			// --整理where语句
			if (!"事务型".equals(modeltype) && !"事务类型".equals(modeltype)) {
				modelGetCdtSql(modelid, datasetid, gmmodelcdt, gmmodelparam,
						gmfunctionchange, updateSqlMap, sqlList);
			} else {
				String tablename = "";
				if (isDataSet) {
					SqlStr = "select sqltablename as tablename from gmdataset where datasetid='"
							+ fdatasetid + "'";
				} else {
					SqlStr = "select physicaltable as tablename from gmmdldts where dtsid='"
							+ fdatasetname + "'";
				}
				ResultSet datasetTableRs = getResultSetBySql(SqlStr,false,
						metaConn);
				while (datasetTableRs.next()) {
					tablename = PublicFunc.reStringByNull(datasetTableRs
							.getString("tablename"));
				}

				if ("".equals(tablename)) {
					return "事务数据模型:" + modelid + "中数据集" + datasetid
							+ "沒有设置保存表,请设置后在生效!";
				}
				Map<String, String> tempMap = new HashMap<String, String>();
				tempMap.put("masterdatasetid", masterdatasetid);
				tempMap.put("masterfields", masterfields);
				tempMap.put("detailfields", detailfields);
				tempMap.put("tablename", tablename);
				datasetlist.put(datasetid, tempMap);

			}
		}

		if (!iscur) {
			return "请在数据模型:" + modelid + "中的设置数据集,后在生效!";
		}

		if ("事务型".equals(modeltype) || "事务类型".equals(modeltype)) {
			// modelDataUpdateLink(modelid, datasetlist, sqlList);
		}

		for (int i = 0; i < sqlList.size(); i++) {
			upDateBySql(sqlList.get(i),false, metaConn);
		}

		return "";
	}
	

	/**
	 * @param modelid
	 * @param metaConn
	 * @throws SQLException
	 */
	// ---处理批号
	private void modeldealbatch(String modelid, Connection metaConn)
			throws SQLException {

		ResultSet gmmodelbatch = null;
		try {
			gmmodelbatch = getResultSetBySql(
							"select a.datasetid,a.batchtype,b.fdatasetname,c.physicaltable,d.propfield  "
									+ "from gmmodelbatch a "
									+ "join gmmodeldataset b on a.modelid=b.modelid and a.datasetid=b.datasetid  "
									+ "join gmmdldts c on b.fdatasetname=c.dtsid "
									+ "join gmprop d on a.batchtype=d.propname "
									+ "where a.modelid='" + modelid + "'",false,
							metaConn);
		} catch (Exception e) {
			log.error("", e);
			return;
		}

		String datasetid = "", batchtype = "", physicaltable = "", propfield = "";
		while (gmmodelbatch.next()) {
			datasetid = PublicFunc.reStringByNull(gmmodelbatch
					.getString("datasetid"));
			batchtype = PublicFunc.reStringByNull(gmmodelbatch
					.getString("batchtype"));
			physicaltable = PublicFunc.reStringByNull(gmmodelbatch
					.getString("physicaltable"));
			propfield = PublicFunc.reStringByNull(gmmodelbatch
					.getString("propfield"));

			if ("".equals(batchtype)) {
				continue;
			}

			if ("".equals(physicaltable)) {
				continue;
			}

			ResultSet gmBatchtype = getResultSetBySql(
					"select physicaltable from gmmdldts where dtsid='"
							+ batchtype + "批号存储'",false, metaConn);
			String batchTable = "";
			while (gmBatchtype.next()) {
				batchTable = PublicFunc.reStringByNull(gmBatchtype
						.getString("physicaltable"));
			}

			String sqlstr = "select " + physicaltable + ".vp_rowid,"
					+ batchTable + "." + propfield;
			for (int i = 1; i <= 10; i++) {
				sqlstr = sqlstr + "," + batchTable + "." + propfield + "cv" + i;
			}
			sqlstr = sqlstr + " from " + physicaltable + " left join "
					+ batchTable + " on " + physicaltable + "." + propfield
					+ "=" + batchTable + "." + propfield;

			String batchStr = " select * from  gmprop where batchtype='"
					+ batchtype + "' ";

			String batchPropStr = "select gmbatchprop.batchgroup,gmbatchprop.batchprop,gmbatchprop.batchtype,gmbatchprop.fieldno,gmbatchprop.viewplugtype,gmprop.datatype  from gmbatchprop join gmprop on gmbatchprop.batchprop=gmprop.propname  where batchtype='"
					+ batchtype + "' order by batchgroup,fieldno ";

			upDateBySql(" update gmmodelbatch set sqlStr='"
					+ sqlstr.replaceAll("'", "''") + "',batchStr='"
					+ batchStr.replaceAll("'", "''") + "',batchPropStr='"
					+ batchPropStr.replaceAll("'", "''") + "' where modelid='"
					+ modelid + "' and datasetid='" + datasetid
					+ "' and batchtype='" + batchtype + "'",false, metaConn);

		}

	}
	
	/**
	 * 整理updatewhere语句
	 * @param gmmodelcdt
	 *            条件数据集
	 * @param gmmodelparam
	 *            参数数据集
	 * @param gmfunctionchange
	 *            方法数据集（不同数据库）
	 * @param updateSqlMap
	 * @throws Exception
	 */
	private void modelGetCdtSql(String modelid, String datasetid,
			ResultSet gmmodelcdt, ResultSet gmmodelparam,
			Map<String, String> gmfunctionchange,
			Map<String, Map<String, String>> updateSqlMap, List<String> sqlList)
			throws Exception {
		
		//检查空字符
		

		// --清空
		Set<String> upclearkey = updateSqlMap.keySet();
		for (Iterator it = upclearkey.iterator(); it.hasNext();) {
			String databaseType = (String) it.next();
			Map<String, String> tempMap = updateSqlMap.get(databaseType);
			tempMap.put("sqlstr", "");
		}

		String updateWhereSql = "", connectchar = "", logic = "", leftparase = "", tablename = "", fieldname = "", paramcaption = "", rightparase = "";

		int parase = 0;
		while (gmmodelcdt.next()) {
			String tempStr = "", paramid = "", notnull = "", notspace = "";
			connectchar = PublicFunc.getLogicStr(gmmodelcdt
					.getString("connectchar"));
			logic = PublicFunc.getLogicStr(gmmodelcdt.getString("logicsymbol"));
			leftparase = PublicFunc.reStringByNull(gmmodelcdt
					.getString("leftparse"));
			tablename = PublicFunc.reStringByNull(gmmodelcdt
					.getString("tablename"));
			fieldname = PublicFunc.reStringByNull(gmmodelcdt
					.getString("fieldname"));
			paramcaption = PublicFunc.reStringByNull(gmmodelcdt
					.getString("paramcaption"));
			rightparase = PublicFunc.reStringByNull(gmmodelcdt
					.getString("rightparse"));
			if (!"".equals(leftparase)) {
				parase = parase + 1;
			}
			if (!"".equals(rightparase)) {
				parase = parase - 1;
			}

			// --查找是否参数出入
			gmmodelparam.beforeFirst();
			while (gmmodelparam.next()) {
				if (paramcaption.equalsIgnoreCase(gmmodelparam
						.getString("paramcaption"))) {
					paramid = gmmodelparam.getString("paramid");
					notnull = gmmodelparam.getString("notnull");
					notspace = gmmodelparam.getString("notspace");
				}
			}

			// --构件where语句
			if (!"".equals(tablename)) {
				fieldname = tablename + "." + fieldname;
			}
			if ("".equals(paramid)) {
				paramcaption = "''" + paramcaption + "''";
			} else {
				if ("1".equals(notspace)) {
					paramcaption = "&" + paramid + "&";
				} else {
					paramcaption = "''&" + paramid + "&''";
				}
			}

			if (logic.indexOf("in") > 0) {
				tempStr = " " + fieldname + " " + logic + " (" + paramcaption
						+ ")";
			} else {
				tempStr = " " + fieldname + " " + logic + " " + paramcaption
						+ " ";
			}

			if (!"1".equals(notnull)) {
				tempStr = "(" + tempStr + " or  &isnull&) ";
			} else {
				tempStr = "(" + tempStr + ")";
			}

			Set<String> key = updateSqlMap.keySet();
			for (Iterator it = key.iterator(); it.hasNext();) {

				String databaseType = (String) it.next();
				Map<String, String> tempMap = updateSqlMap.get(databaseType);
				String sqlwhere = tempMap.get("sqlstr");
				if ("".equals(sqlwhere)) {
					connectchar = " ";
				}
				sqlwhere = sqlwhere + connectchar + leftparase + tempStr
						+ rightparase;

				if (!"1".equals(notnull)) {
					for (Entry<String, String> eMap : gmfunctionchange.entrySet()) {
						if (databaseType.equalsIgnoreCase(eMap.getKey())) {
							sqlwhere = sqlwhere.replaceAll("&isnull&",
									"("+ eMap.getValue().replaceAll("&field&", paramcaption) + ")");
						}
					}
				}
				tempMap.put("sqlstr", sqlwhere);
			}

		}

		if (parase != 0) {
			throw new Exception("数据模型条件括号不对称，请检查");
		}
		for (Iterator it = upclearkey.iterator(); it.hasNext();) {
			String databaseType = (String) it.next();
			Map<String, String> tempMap = updateSqlMap.get(databaseType);

			if (!"".equals(updateWhereSql)) {
				updateWhereSql = updateWhereSql + ",";
			}
			String tempStr = tempMap.get("sqlstr");
			updateWhereSql = updateWhereSql + databaseType + "wheresql='"
					+ tempStr + "'";
		}

		sqlList.add("update gmmodeldataset set " + updateWhereSql
				+ " where modelid='" + modelid + "' and datasetid='"
				+ datasetid + "'");
	}

	
	private CachedRowSet ResultSetToCachedRowSet(ResultSet rs) throws SQLException{
		CachedRowSet crs = new CachedRowSetImpl();
		crs.populate(rs);
		rs.getStatement().close();
		return crs;
	}
	
	private ResultSet getResultSetBySql(String sql,Boolean scrollsensitive, Connection conn) throws SQLException{
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}
	
	private void upDateBySql(String sql, Boolean scrollsensitive, Connection conn) throws SQLException{
		Statement stmt = conn.createStatement();
		stmt.execute(sql);
		stmt.close();
	}

}
