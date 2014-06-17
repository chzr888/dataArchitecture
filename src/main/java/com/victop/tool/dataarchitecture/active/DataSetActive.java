package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.rowset.CachedRowSetImpl;
import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * "数据集生效"
 * @author chenzr
 */
public class DataSetActive extends Active {
	
	private Logger logger = LoggerFactory.getLogger(DataSetActive.class);
	
	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		String datasetid = resourceid;
		logger.info("数据集："+resourceid);
		try {

			ResultSet GMdataset_rs = getResultSetBySql(
					"select * from GMdataset where datasetid='" + datasetid
							+ "'",false, H2Conn);
			ResultSet GMdataset = ResultSetToCachedRowSet(GMdataset_rs);
			// ---检查数据集有效性
			String reMsgString = dataSetActiveCheck(GMdataset, H2Conn);
			if (!"".equals(reMsgString)) {
				throw new Exception(reMsgString);
			}

			// 维护后台表数据-------------------------------------------------------------------------------------------
			reMsgString = dataSetDealData(GMdataset, H2Conn);
			if (!"".equals(reMsgString)) {
				throw new Exception(reMsgString);
			}

			// ----修改数据集状态
			upDateBySql(
					"update GMdataset set actived=1  where datasetid='"
							+ datasetid + "'",false, H2Conn);

		} catch (Exception e) {
			e.printStackTrace();
			// --释放连接
			return datasetid + "生效失败 【"+e.getMessage()+"】";

		}
		return datasetid + "生效成功";
	}
	
	/**
	 * 检查数据集设置
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private String dataSetActiveCheck(ResultSet GMdataset, Connection metaConn)
			throws SQLException {
		boolean iscur = false;

		String datasetid = "", datasettypebusi = "", datasettype = "", sqltablename = "", actived = "";
		GMdataset.beforeFirst();
		while (GMdataset.next()) {
			datasetid = PublicFunc.reStringByNull(GMdataset
					.getString("datasetid"));
			datasettypebusi = PublicFunc.reStringByNull(GMdataset
					.getString("datasettypebusi"));
			datasettype = PublicFunc.reStringByNull(GMdataset
					.getString("datasettype"));
			sqltablename = PublicFunc.reStringByNull(GMdataset
					.getString("sqltablename"));
			actived = PublicFunc.reStringByNull(GMdataset.getString("actived"));
			iscur = true;
		}

		if (!iscur) {
			return "请保存数据集后再生效!";
		}
		// if ("1".equals(actived)) {
		// return "数据集已经生效!";
		// }
		if ("事务型".equals(datasettypebusi)) {
			if ("常规数据集".equals(datasettype)) {
				return "事务型数据集不能设置，常规数据集!";
			}
		}
		if ("事务型".equals(datasettypebusi)) {
			if ("".equals(sqltablename)) {
				return "事务型数据集请设置保存物理表名!";
			}
		}
		return "";
	}

	/**
	 * @param GMdataset
	 * @param metaConn
	 * @return
	 * @throws Exception
	 */
	// ---整理数据集后台数据
	private String dataSetDealData(ResultSet GMdataset, Connection metaConn)
			throws Exception {

		String dataSetID = "", datasettype = "", sqltablename = "", keyStr = "", sqlStr = "";
		GMdataset.beforeFirst();
		while (GMdataset.next()) {
			dataSetID = GMdataset.getString("dataSetID");
			datasettype = PublicFunc.reStringByNull(GMdataset
					.getString("datasettype"));
			sqltablename = PublicFunc.reStringByNull(GMdataset
					.getString("sqltablename"));
		}

		// --存储sql语句
		List<String> sqlList = new ArrayList<String>();

		if ("sql数据集".equalsIgnoreCase(datasettype)) {
			// ---整理2维表设置
			getDealDataSetChangeRs(metaConn, dataSetID, sqlList);
			for (int i = 0; i < sqlList.size(); i++) {
				upDateBySql(sqlList.get(i),false, metaConn);
			}
			return "";
		}

		// --删除之前设置
		sqlList.add("delete from gmdatastore where datasetid='" + dataSetID
				+ "'");
		// --角度
		sqlList
				.add("insert into gmdatastore(datasetid,fieldcaption,fieldname,datatype,tablename,mastername,propname,no,iskey)"
						+ "select '"
						+ dataSetID
						+ "',b.masterprop,c.propfield,c.datatype,b.mastertablename,d.mastername,d.propname,0,1 "
						+ "from gmdatasetdim  a "
						+ "join gmmaster  b on  a.dimprop=b.masterprop  "
						+ "join gmprop c on b.masterprop=c.propname "
						+ "left join gmqfmaster d on b.mastername=d.mastername and b.masterprop=d.propname"
						+ " where a.datasetid='" + dataSetID + "'");

		// ---跟随属性
		sqlList
				.add("insert into gmdatastore(datasetid,fieldcaption,fieldname,datatype,tablename,mastername,propname,no)"
						+ "select datasetid,a.propname,a.propfield,b.datatype,'',case when not isnull(d.mastername) then d.mastername else f.mastername end as qmastername, "
						+ "case when not isnull(d.propname) then d.propname else f.propname end as qpropname,'0'   "
						+ "from gmdatasetfollowprop  a   "
						+ "join gmmaster n on a.dimprop=n.masterprop "
						+ "join gmprop b on  a.propname=b.propname  "
						+ "left join  gmdimlink c on a.dimgroup=c.dimgroup and a.propname=c.propname  "
						+ "left join  gmqfmaster d on c.mastername=d.mastername and c.propname=d.propname "
						+ "left join  gmqfmaster f on n.mastername=f.mastername and a.propname=f.propname	"
						+ "where a.datasetid='" + dataSetID + "'");

		// ----数据项
		sqlList
				.add("insert into gmdatastore(datasetid,fieldcaption,fieldname,datatype,tablename,mastername,propname,no)"
						+ " select a.datasetid,a.caption,case when not isnull(a.fieldname) and a.fieldname<>'' then a.fieldname else b.columnid end as columnid,b.datatype,'','','',0"
						+ " from gmdatasetfield a left join gmdtstablecolumn b on  a.dtsid=b.dtsid and a.fieldid=b.columncaption "
						+ "where a.datasetid='" + dataSetID + "'");

		// ------构建sql语句
		Map<String, String> sqlMap = new HashMap<String, String>(); // ---结果
		sqlMap.put("selectstr", "");
		sqlMap.put("tablestr", "");
		sqlMap.put("groupstr", "");
		sqlMap.put("isgroup", "0");

		Map<String, Map<String, String>> columnMap = new HashMap<String, Map<String, String>>(); // ---字段列表
		Map<String, String> DtsJoinMap = new HashMap<String, String>(); // ---角度字段列表

		// 所有角度
		Map<String, Map<String, String>> dimNameMap = new HashMap<String, Map<String, String>>();
		String baseDtsid = "", baseDtsTableNAme = "", baseDtsdeal = "";

		// --数据集角度
		ResultSet gmdatasetdim_rs = getResultSetBySql(
				"select dimprop,used from gmdatasetdim  "
						+ " where datasetid='" + dataSetID + "'", false, metaConn);
		ResultSet gmdatasetdim = ResultSetToCachedRowSet( gmdatasetdim_rs );
		while (gmdatasetdim.next()) {
			Map<String, String> temp = new HashMap<String, String>();
			temp.put("datasetdim", "1");
			temp.put("used", gmdatasetdim.getString("used"));
			temp.put("tablename", "");
			temp.put("field", "");
			temp.put("dtsid", "");
			temp.put("columnid", "");
			dimNameMap.put(gmdatasetdim.getString("dimprop"), temp);
		}

		// --数据原表
		ResultSet gmdatasetdts_rs = getResultSetBySql(
						"select a.dtsid,a.filter,a.dtsdeal,b.physicaltable from gmdatasetdts a "
								+ " join gmmdldts b on  a.dtsid=b.dtsid where a.datasetid='"
								+ dataSetID + "'", false, metaConn);
		ResultSet gmdatasetdts = ResultSetToCachedRowSet(gmdatasetdts_rs);
		while (gmdatasetdts.next()) {
			if ("全部".equals(gmdatasetdts.getString("filter"))) {
				baseDtsid = gmdatasetdts.getString("dtsid");
				baseDtsTableNAme = gmdatasetdts.getString("physicaltable");
				baseDtsdeal = gmdatasetdts.getString("dtsdeal");
			}
		}

		// --基本表
		if (!"".equals(baseDtsid)) {
			datasetDealDts(baseDtsid, baseDtsTableNAme, baseDtsdeal, baseDtsid,
					dataSetID, metaConn, dimNameMap, sqlMap, DtsJoinMap,
					columnMap);
		}

		gmdatasetdts.beforeFirst();
		while (gmdatasetdts.next()) {
			if ("全部".equals(gmdatasetdts.getString("filter"))) {
				continue;
			}
			datasetDealDts(gmdatasetdts.getString("dtsid"), gmdatasetdts
					.getString("physicaltable"), gmdatasetdts
					.getString("dtsdeal"), baseDtsid, dataSetID, metaConn,
					dimNameMap, sqlMap, DtsJoinMap, columnMap);
		}

		// ---整理跟随属性
		dealdatasetfollow(dataSetID, metaConn, dimNameMap, sqlMap, columnMap);

		// ---构建sql语句 sql92
		sqlList.add("delete from gmdatasetsql where datasetid='" + dataSetID
				+ "'");

		// --整理sql语句
		sqlStr = dealDatasetsql(baseDtsid, dataSetID, dimNameMap, sqlMap,
				DtsJoinMap, columnMap, sqlList, keyStr);

		sqlList
				.add("insert into gmdatasetsql(datasetid,databasetype,sqlstr,isnew)values('"
						+ dataSetID + "','sql92','" + sqlStr + "',1)");

		// ---整理2维表设置
		getDealDataSetChangeRs(metaConn, dataSetID, sqlList);

		for (int i = 0; i < sqlList.size(); i++) {
			upDateBySql(sqlList.get(i),false, metaConn);
		}

		return "";
	}

	/**
	 * @param metaConn
	 * @param datasetid
	 * @param sqllist
	 * @throws SQLException
	 */
	// ----整理二维表（简单）
	private void getDealDataSetChangeRs(Connection metaConn, String datasetid,
			List<String> sqllist) throws SQLException {

		ResultSet list_rs = getResultSetBySql(
						"select * from gmdatastore where datasetid='"
								+ datasetid + "'",false, metaConn);
		ResultSet list = ResultSetToCachedRowSet(list_rs);
		boolean findChange = false;
		String createSql = "";
		Map<String, String> fieldMap = new HashMap<String, String>();
		List<String> keylist = new ArrayList<String>();

		String datarevolvefield = "";
		while (list.next()) {
			datarevolvefield = PublicFunc.reStringByNull(list
					.getString("datarevolvefield"));

			if ("1".equals(list.getString("iskey"))) {
				keylist.add(list.getString("fieldname"));
			}

			if (!"".equals(datarevolvefield)) {
				// ---找不到
				findChange = true;
				if ("".equals(PublicFunc.reStringByNull(fieldMap
						.get(datarevolvefield)))) {
					fieldMap.put(PublicFunc.reStringByNull(list
							.getString("fieldcaption")), list
							.getString("fieldname"));
				}
				continue;
			}

			if (!"".equals(createSql)) {
				createSql = createSql + ",";
			}
			createSql = createSql + list.getString("fieldname") + " ";
		}

		if (!findChange) {
			sqllist.add("update gmdataset set datarevolve=0  where datasetid='"
					+ datasetid + "'");
			logger.info("没有找到2维表设置");
			return;
		}

		sqllist.add("update gmdataset set datarevolve=1  where datasetid='"
				+ datasetid + "'");

		int no = -1;
		sqllist.add("delete from gmDataSetRevolveSql where datasetid='"
				+ datasetid + "'");
		String tempStr = "insert into T" + datasetid + "(" + createSql
				+ ") select distinct " + createSql + " from T" + datasetid
				+ "_temp ";
		sqllist
				.add("insert into gmDataSetRevolveSql(datasetid,no,sqlstr)values('"
						+ datasetid + "'," + no + ",'" + tempStr + "')");

		String join = "";

		// --构建关系
		for (int i = 0; i < keylist.size(); i++) {
			if (!"".equals(join)) {
				join = join + " and ";
			}
			join = join + " a." + keylist.get(i) + "=b." + keylist.get(i);
		}

		Set<String> key = fieldMap.keySet();
		for (Iterator it = key.iterator(); it.hasNext();) {
			String s = (String) it.next();

			no = no + 1;
			tempStr = "update T" + datasetid + " a set a." + fieldMap.get(s)
					+ "=(select b.floatvalue from T" + datasetid
					+ "_temp b where " + join + " and b.itemname='" + s + "' )";

			sqllist
					.add("insert into gmDataSetRevolveSql(datasetid,no,sqlstr)values('"
							+ datasetid
							+ "',"
							+ no
							+ ",'"
							+ tempStr.replaceAll("'", "''") + "')");
		}

	}


	/**
	 * 整理数据原
	 *  
	 * @param dtsID
	 *            数据原表ID
	 * @param tableNAme
	 *            物理表名
	 * @param Dtsdeal
	 *            处理
	 * @param baseDtsID
	 *            基础数据原表ID
	 * @param dataSetID
	 *            数据集ID
	 * @param metaConn
	 *            元数据库
	 * @param dimNameMap
	 *            角度
	 * @param sqlMap
	 *            sql语句
	 * @param DtsJoinMap
	 *            数据集 Join
	 * @param columnMap
	 *            字段信息
	 * @throws Exception
	 */
	private void datasetDealDts(String dtsID, String tableNAme, String Dtsdeal,
			String baseDtsID, String dataSetID, Connection metaConn,
			Map<String, Map<String, String>> dimNameMap,
			Map<String, String> sqlMap, Map<String, String> DtsJoinMap,
			Map<String, Map<String, String>> columnMap) throws Exception {

		// --区别基础数据原表
		boolean isBase = false, haveDim = false, isDimdts = false;
		String isgroup = sqlMap.get("isgroup");
		String selectstr = sqlMap.get("selectstr");
		String tablestr = sqlMap.get("tablestr");
		String groupstr = sqlMap.get("groupstr");

		if (dtsID.equalsIgnoreCase(baseDtsID)) {
			isBase = true;
		}

		if ("".equals(tablestr)) {
			tablestr = " " + tableNAme + " ";
		}

		if ("角度损失".equals(Dtsdeal)) {
			if (!"1".equals(isgroup)) {
				isgroup = "1";
				sqlMap.put("isgroup", "1");
			}
		}

		// --基本表角度
		ResultSet gmdatasetdtsdim_rs = getResultSetBySql(
						"select a.dimprop,c.mastername,c.isoutside,c.masterprop,b.dimfield,a.dimgroup,e.mastername as dimgroupmaster,e.masterprop as dimgroupprop,e.mastertablename as dimgroupTableName,f.propfield as dimgroupfield "
								+ "from gmdatasetdtsdim  a  "
								+ "join gmmaster c on a.dimprop=c.masterprop "
								+ "join gmdtsdim b on  a.dtsid=b.dtsid and c.mastername=b.dimname "
								+ "left join gmmaster e on a.dimgroupprop=e.masterprop "
								+ "left join gmprop f on e.masterprop=f.propname "
								+ "where a.datasetid='"
								+ dataSetID
								+ "' and a.dtsid='" + dtsID + "' and a.used=1",false,
						metaConn);
		ResultSet gmdatasetdtsdim = ResultSetToCachedRowSet(gmdatasetdtsdim_rs);
		String dimprop, dimmastername, dimfield, dimgroup, dimgroupmaster, dimgroupprop, dimgroupTableName, dimgroupfield, joinStr = "";

		// --循环角度
		while (gmdatasetdtsdim.next()) {
			haveDim = true;
			dimprop = gmdatasetdtsdim.getString("dimprop"); // --角度属性
			dimmastername = gmdatasetdtsdim.getString("mastername"); // --角度

			dimfield = gmdatasetdtsdim.getString("dimfield"); // --角度字段
			dimgroup = PublicFunc.reStringByNull(gmdatasetdtsdim
					.getString("dimgroup")); // --角度组
			dimgroupmaster = PublicFunc.reStringByNull(gmdatasetdtsdim
					.getString("dimgroupmaster")); // --扩展角度
			dimgroupprop = gmdatasetdtsdim.getString("dimgroupprop"); // --扩展属性
			dimgroupTableName = gmdatasetdtsdim.getString("dimgroupTableName");// --扩展表
			dimgroupfield = gmdatasetdtsdim.getString("dimgroupfield"); // --字段

			// ---存在扩展角度
			if (!"".equals(dimgroup) && !"".equals(dimgroupmaster)) {
				if (!"1".equals(isgroup)) {
					isgroup = "1";
					sqlMap.put("isgroup", "1");
				}

				// ---找出角度组与数据源相关的角度no
				String qdimno = "", qmastername = "", qpropname = "", qmainprop = "", qcolumnid = "", qisoutside = "", qmastertablename = "";
				ResultSet findlinkDim_rs = getResultSetBySql(
								"select a.dimno,a.mastername,a.propname,b.mainprop,b.columnid,c.isoutside,c.mastertablename "
										+ "from gmdimlink  a join gmmastertablecolumn b "
										+ "on  a.mastername=b.mastername and a.propname=b.columncaption "
										+ " join gmmaster c on a.mastername=c.mastername"
										+ "  where a.dimgroup='"
										+ dimgroup
										+ "'  and  a.mastername='"
										+ dimmastername
										+ "' order by dimno limit 1",false, metaConn);
				ResultSet findlinkDim = ResultSetToCachedRowSet(findlinkDim_rs);
				
				while (findlinkDim.next()) {
					qdimno = PublicFunc.reStringByNull(findlinkDim
							.getString("dimno"));
					qmastername = PublicFunc.reStringByNull(findlinkDim
							.getString("mastername"));
					qpropname = PublicFunc.reStringByNull(findlinkDim
							.getString("propname"));
					qmainprop = PublicFunc.reStringByNull(findlinkDim
							.getString("mainprop"));
					qcolumnid = PublicFunc.reStringByNull(findlinkDim
							.getString("columnid"));
					qisoutside = PublicFunc.reStringByNull(findlinkDim
							.getString("isoutside"));
					qmastertablename = PublicFunc.reStringByNull(findlinkDim
							.getString("mastertablename"));
				}

				String fTableName = "", fField = "", fdtsid = dtsID, columnid = "", isoutside = ""; // ---初始值为数据角度表，字段。
				String fmainprop = "", fpropdatafield = "";

				// --需要连接主档数据
				if ("".equals(qmainprop) && !"1".equals(qisoutside)) {

					// --外部数据

					tablestr = tablestr + " join " + qmastertablename + " on "
							+ tableNAme + "." + dimfield + "="
							+ qmastertablename + ".mastervalue ";

					fTableName = qmastertablename;
					fField = "ID";
					columnid = qcolumnid;
				} else {
					fTableName = tableNAme;
					fField = dimfield;
					columnid = dimfield;
				}

				ResultSet dimGroupRs_rs = getResultSetBySql(
								"select gmdimlink.propname,gmdimlink.mastername,lowsql,gmmaster.mastertablename,gmprop.propfield,"
										+ "gmmaster.isoutside,d.mainprop,f.propdatafield,gmmaster.masterprop as qpropname  "
										+ "from gmdimlink  "
										+ "left join gmmaster on gmdimlink.mastername=gmmaster.mastername "
										+ "left join gmprop on gmdimlink.propname=gmprop.propname "
										+ "join gmmastertablecolumn d on gmdimlink.mastername=d.mastername and gmdimlink.propname=d.columncaption "
										+ "join gmdatatype f on d.datatype=f.datatype "
										+ "where dimgroup='"
										+ dimgroup
										+ "' "
										+ "and dimno>= "
										+ qdimno
										+ "  and dimno<=(select dimno from gmdimlink  where dimgroup='"
										+ dimgroup
										+ "' and propname='"
										+ dimgroupprop
										+ "' and mastername='"
										+ dimgroupmaster
										+ "') "
										+ "order by dimno",false,  metaConn);
				ResultSet dimGroupRs = ResultSetToCachedRowSet(dimGroupRs_rs);
				while (dimGroupRs.next()) {
					String rsPropName = dimGroupRs.getString("propname");
					String lowsql = PublicFunc.reStringByNull(dimGroupRs
							.getString("lowsql"));
					fmainprop = PublicFunc.reStringByNull(dimGroupRs
							.getString("mainprop"));
					fpropdatafield = PublicFunc.reStringByNull(dimGroupRs
							.getString("propdatafield"));

					if (!"mastervalue".equalsIgnoreCase(fmainprop)) {
						rsPropName = PublicFunc.reStringByNull(dimGroupRs
								.getString("qpropname"));
					}

					if ("".equals(fdtsid)) {
						fTableName = dimGroupRs.getString("mastertablename");
						columnid = dimGroupRs.getString("propfield");
						isoutside = dimGroupRs.getString("isoutside");
						if ("1".equals(isoutside)) {
							fField = columnid;
						} else {
							if ("".equals(fmainprop)) {
								fField = fpropdatafield;
								fTableName = fTableName + "_prop";
							} else {
								fField = fmainprop;
							}
						}
					}

					Map<String, String> tempDimMap = dimNameMap.get(rsPropName);

					// --没有找到角度
					if (tempDimMap == null) {
						if (!"".equals(lowsql)) {
							tablestr = tablestr
									+ lowsql.replace("&tablename&", fTableName)
											.replace("&field&", fField);
						}
						Map<String, String> map = new HashMap<String, String>();
						map.put("datasetdim", "0");
						map.put("tablename", fTableName);
						map.put("field", fField);
						map.put("columnid", columnid);
						map.put("dtsid", fdtsid);
						dimNameMap.put(rsPropName, map);
					} else {
						// --第一次更新角度
						if ("".equals(tempDimMap.get("tablename"))) {
							tempDimMap.put("tablename", fTableName);
							if (fTableName.equalsIgnoreCase(tableNAme)) {
								tempDimMap.put("field", columnid);
								tempDimMap.put("dtsid", dtsID);
								isDimdts = true;
								tempDimMap.put("columnid", columnid);
							} else {
								tempDimMap.put("field", fField);
								tempDimMap.put("dtsid", "");
								tempDimMap.put("columnid", columnid);
							}
							if (!"".equals(lowsql)) {
								tablestr = tablestr
										+ lowsql
												.replace(
														"&tablename&",
														tempDimMap
																.get("tablename"))
												.replace("&field&",
														tempDimMap.get("field"));
							}
						}

					}

					fdtsid = "";
				}

			}
			// --没有扩展角度
			else {
				Map<String, String> tempDimMap = dimNameMap.get(dimprop);
				if ("".equals(tempDimMap.get("tablename"))) {
					tempDimMap.put("tablename", tableNAme);
					tempDimMap.put("columnid", dimfield);
					tempDimMap.put("dtsid", dtsID);
					tempDimMap.put("field", dimfield);

					isDimdts = true;
				}
			}

			// ---join
			Map<String, String> tempDimMapfin = dimNameMap.get(dimprop);
			joinStr = PublicFunc.reConnectString(joinStr, tempDimMapfin
					.get("tablename")
					+ "."
					+ tempDimMapfin.get("field")
					+ "="
					+ tableNAme
					+ "."
					+ dimfield, " and ");
		}

		// --需要数据源表作為角度
		if (isDimdts) {
			if (tablestr.toLowerCase().indexOf(tableNAme.toLowerCase()) < 0) {
				String joinlin = " join ";
				if (!"".equals(baseDtsID)) {
					joinlin = " left join ";
				}
				tablestr = tablestr + " " + joinlin + " " + tableNAme + " ";
			}
		}
		// --不在角度里面
		if (tablestr.toLowerCase().indexOf(tableNAme.toLowerCase()) < 0) {
			String joinlin = " join ";
			if (!"".equals(baseDtsID)) {
				joinlin = " left join ";
			}
			DtsJoinMap.put(dtsID, joinlin + tableNAme + "  on  " + joinStr);
		}
		if (!haveDim) {
			throw new Exception("数据原表：" + dtsID + ",没有角度匹配，请设置该数据原表");
		}

		// ---整理数据列
		ResultSet dtscolumn_rs = getResultSetBySql(
						"select a.datasetid,a.caption,a.fieldname, b.columnid,b.datatype,control "
								+ " from gmdatasetfield a left join gmdtstablecolumn b on  a.dtsid=b.dtsid and a.fieldid=b.columncaption  "
								+ " where a.datasetid='" + dataSetID
								+ "' and a.dtsid='" + dtsID + "'", false, metaConn);
		ResultSet dtscolumn = ResultSetToCachedRowSet( dtscolumn_rs);
		while (dtscolumn.next()) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("tablename", tableNAme);
			map.put("control", getcontrolsql(dtscolumn.getString("control")));
			map.put("columnid", PublicFunc.reStringByNull(dtscolumn
					.getString("fieldname")));
			map.put("fieldname", dtscolumn.getString("columnid"));
			columnMap.put(dtscolumn.getString("caption"), map);
		}

		// ---跟随属性数据
		ResultSet dtsfollow_rs = getResultSetBySql(
						"select a.propname,a.propfield, b.columnid,'' as mainprop "
								+ "from gmdatasetfollowprop a  "
								+ "join gmdtstablecolumn b on  a.dtsid=b.dtsid and a.propname=b.columncaption  "
								+ " where a.datasetid='" + dataSetID
								+ "' and a.dtsid='" + dtsID + "'",false, metaConn);
		ResultSet dtsfollow = ResultSetToCachedRowSet(dtsfollow_rs);
		while (dtsfollow.next()) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("tablename", tableNAme);
			map.put("control", "");
			map.put("columnid", PublicFunc.reStringByNull(dtsfollow
					.getString("propfield")));
			if ("".equals(dtsfollow.getString("mainprop"))) {
				map.put("fieldname", dtsfollow.getString("columnid"));
			} else {
				map.put("fieldname", dtsfollow.getString("mainprop"));
			}
			columnMap.put(dtsfollow.getString("propname"), map);
		}

		sqlMap.put("selectstr", selectstr);
		sqlMap.put("tablestr", tablestr);

	}
	
	/**
	 * 更新跟随属性
	 * 
	 * @param dataSetID
	 * @param metaConn
	 * @param dimNameMap
	 * @param sqlMap
	 * @param columnMap
	 * @throws SQLException
	 */
	private void dealdatasetfollow(String dataSetID, Connection metaConn,
			Map<String, Map<String, String>> dimNameMap,
			Map<String, String> sqlMap,
			Map<String, Map<String, String>> columnMap) throws SQLException {

		String isgroup = sqlMap.get("isgroup");
		String selectstr = sqlMap.get("selectstr");
		String tablestr = sqlMap.get("tablestr");
		String groupstr = sqlMap.get("groupstr");

		ResultSet rs_rs = getResultSetBySql(
						"select a.dimprop as dimname,a.dimgroup,a.propname,a.propfield,b.masterprop,b.mastertablename,b.isoutside,c.mainprop,c.columnid,d.propdatafield "
								+ " from  gmdatasetfollowprop  a "
								+ " join gmmaster b on  a.dimprop=b.masterprop"
								+ " join gmmastertablecolumn c on b.mastername=c.mastername and a.propname=c.columncaption"
								+ " join gmdatatype d on c.datatype=d.datatype"
								+ " where datasetid='"
								+ dataSetID
								+ "' and (dtsid='' or IFNULL(dtsid, true))", false, metaConn);
		ResultSet rs = ResultSetToCachedRowSet(rs_rs);
		String dimgroup, masterprop, mastertablename, dimgroupprop, dimname, tableNAme, dimfield, mainprop, columnid, propfield, propname, propdatafield;
		while (rs.next()) {
			dimgroup = PublicFunc.reStringByNull(rs.getString("dimgroup"));
			masterprop = PublicFunc.reStringByNull(rs.getString("masterprop"));
			mastertablename = PublicFunc.reStringByNull(rs
					.getString("mastertablename"));
			dimgroupprop = PublicFunc.reStringByNull(rs.getString("propname"));
			dimname = PublicFunc.reStringByNull(rs.getString("dimname"));
			mainprop = PublicFunc.reStringByNull(rs.getString("mainprop"));
			//
			propname = PublicFunc.reStringByNull(rs.getString("propname"));
			propfield = PublicFunc.reStringByNull(rs.getString("propfield"));
			columnid = PublicFunc.reStringByNull(rs.getString("columnid"));
			propdatafield = PublicFunc.reStringByNull(rs
					.getString("propdatafield"));

			tableNAme = dimNameMap.get(dimname).get("tablename");
			dimfield = dimNameMap.get(dimname).get("field");

			if (!"".equals(dimgroup)) {
				ResultSet dimGroupRs_rs = getResultSetBySql(
								"select gmdimlink.mastername,lowsql,gmmaster.mastertablename,gmprop.propfield,gmmaster.isoutside  "
										+ "from gmdimlink  "
										+ "left join gmmaster on gmdimlink.mastername=gmmaster.mastername "
										+ "left join gmprop on gmdimlink.masterprop=gmprop.propname "
										+ "where dimgroup='"
										+ dimgroup
										+ "' "
										+ "and dimno>=(select dimno from gmdimlink  where dimgroup='"
										+ dimgroup
										+ "' and propname='"
										+ masterprop
										+ "') "
										+ "and dimno<=(select dimno from gmdimlink  where dimgroup='"
										+ dimgroup
										+ "' and propname='"
										+ dimgroupprop
										+ "') "
										+ "order by dimno", false, metaConn);
				ResultSet dimGroupRs = ResultSetToCachedRowSet(dimGroupRs_rs);
				String fTableName = tableNAme, fField = dimfield, fcolumnid = dimfield, isoutside = ""; // ---初始值为数据角度表，字段。
				while (dimGroupRs.next()) {

					String RsMasterName = dimGroupRs.getString("mastername");
					String lowsql = PublicFunc.reStringByNull(dimGroupRs
							.getString("lowsql"));
					Map<String, String> tempDimMap = dimNameMap
							.get(RsMasterName);

					// --没有找到角度
					if (tempDimMap == null) {
						if (!"".equals(lowsql)) {
							tablestr = tablestr
									+ lowsql.replace("&tablename&", fTableName)
											.replace("&field&", fField);
						}
						Map<String, String> map = new HashMap<String, String>();
						map.put("datasetdim", "0");
						map.put("tablename", fTableName);
						map.put("field", fField);
						map.put("columnid", fcolumnid);
						map.put("dtsid", "");
						dimNameMap.put(RsMasterName, map);
					} else {
						// --第一次更新角度
						if ("".equals(tempDimMap.get("tablename"))) {
							tempDimMap.put("tablename", fTableName);
							tempDimMap.put("field", fField);
							tempDimMap.put("dtsid", "");
							tempDimMap.put("columnid", fcolumnid);
							if (!"".equals(lowsql)) {
								tablestr = tablestr
										+ lowsql
												.replace(
														"&tablename&",
														tempDimMap
																.get("tablename"))
												.replace("&field&",
														tempDimMap.get("field"));
							}
						}

					}

					fTableName = dimGroupRs.getString("mastertablename");
					columnid = dimGroupRs.getString("propfield");
					if ("1".equals(dimGroupRs.getString("isoutside"))) {
						fField = columnid;
					} else {
						fField = "mastervalue";
					}
					isoutside = dimGroupRs.getString("isoutside");
				}

				if ("1".equals(isoutside)) {
					Map<String, String> map = new HashMap<String, String>();
					map.put("tablename", fTableName);
					map.put("control", "");
					map.put("columnid", fcolumnid);
					map.put("fieldname", fField);
					columnMap.put(propname, map);
				} else {

					String propTableName = "T" + fcolumnid;
					tablestr = tablestr + " left  join " + fTableName
							+ "_prop as " + propTableName + " on " + fTableName
							+ ".ID=" + propTableName + ".ID  and  "
							+ propTableName + ".propname='" + propname + "'  ";
					Map<String, String> map = new HashMap<String, String>();
					map.put("tablename", propTableName);
					map.put("control", "");
					map.put("columnid", fcolumnid);
					map.put("fieldname", fField);
					columnMap.put(propname, map);

				}

			} else {

				// --外部数据
				if ("1".equals(PublicFunc.reStringByNull(rs
						.getString("isoutside")))) {

					Map<String, String> map = new HashMap<String, String>();
					map.put("tablename", tableNAme);
					map.put("control", "");
					map.put("columnid", columnid);
					map.put("fieldname", PublicFunc.reStringByNull(propfield));
					columnMap.put(propname, map);

				} else {
					if (tablestr.toLowerCase().indexOf(
							" " + mastertablename.toLowerCase() + " ") < 0) {
						tablestr = tablestr + " join " + mastertablename
								+ " on  " + tableNAme + "." + dimfield + "="
								+ mastertablename + ".mastervalue ";
					}

					if (!"".equals(mainprop)) {
						Map<String, String> map = new HashMap<String, String>();
						map.put("tablename", mastertablename);
						map.put("control", "");
						map.put("columnid", columnid);
						map.put("fieldname", PublicFunc
								.reStringByNull(mainprop));
						columnMap.put(propname, map);
					} else {
						String propTableName = "T" + columnid;
						tablestr = tablestr + " left  join " + mastertablename
								+ "_prop as " + propTableName + " on "
								+ mastertablename + ".ID=" + propTableName
								+ ".ID  and  " + propTableName + ".propname='"
								+ propname + "'  ";
						Map<String, String> map = new HashMap<String, String>();
						map.put("tablename", propTableName);
						map.put("control", "");
						map.put("columnid", columnid);
						map.put("fieldname", PublicFunc
								.reStringByNull(propdatafield));
						columnMap.put(propname, map);
					}
				}
			}
		}

		sqlMap.put("selectstr", selectstr);
		sqlMap.put("tablestr", tablestr);
	}
	
	/**
	 * 整理sql
	 
	 * @param baseDtsID
	 *            基础数据原表
	 * @param dimNameMap
	 *            角度map
	 * @param sqlMap
	 *            sqlmap
	 * @param DtsJoinMap
	 *            join语句
	 * @param columnMap
	 *            列表
	 */
	private String dealDatasetsql(String baseDtsID, String datasetId,
			Map<String, Map<String, String>> dimNameMap,
			Map<String, String> sqlMap, Map<String, String> DtsJoinMap,
			Map<String, Map<String, String>> columnMap, List<String> sqlList,
			String keyStr) {

		String isgroup = sqlMap.get("isgroup");
		String selectstr = sqlMap.get("selectstr");
		String tablestr = sqlMap.get("tablestr");
		String groupstr = sqlMap.get("groupstr");

		Set<String> key = DtsJoinMap.keySet();
		for (Iterator it = key.iterator(); it.hasNext();) {
			String dtsID = (String) it.next();
			String joinstr = DtsJoinMap.get(dtsID);
			tablestr = tablestr + " " + joinstr;
		}

		// 角度
		Set<String> Dimkey = dimNameMap.keySet();
		for (Iterator it = Dimkey.iterator(); it.hasNext();) {
			String dimName = (String) it.next();
			Map<String, String> temp = dimNameMap.get(dimName);
			String ATableName = temp.get("tablename");
			String AsColumnID = temp.get("columnid");
			String Afield = temp.get("field");
			String dtsid = temp.get("dtsid");

			if (Afield.equalsIgnoreCase(AsColumnID)) {
				AsColumnID = "";
			} else {
				AsColumnID = " as " + AsColumnID + " ";
			}
			if ("1".equals(temp.get("datasetdim"))) {
				if (!"".equals(selectstr)) {
					selectstr = selectstr + ",";
					groupstr = groupstr + ",";
				}
				selectstr = selectstr + temp.get("tablename") + "." + Afield
						+ "" + AsColumnID;
				groupstr = groupstr + temp.get("tablename") + "." + Afield;

				sqlList.add("update gmdatastore set tablename='" + ATableName
						+ "' where datasetid='" + datasetId
						+ "' and fieldname='" + temp.get("columnid") + "'");
			}

		}

		// --数据列
		Set<String> columnKey = columnMap.keySet();
		for (Iterator it = columnKey.iterator(); it.hasNext();) {

			String columnCaption = (String) it.next();
			Map<String, String> map = columnMap.get(columnCaption);
			String tablename = map.get("tablename");
			String control = map.get("control");
			String columnid = map.get("columnid"); // as 字段
			String fieldname = map.get("fieldname"); // 主档字段

			if (columnid.equalsIgnoreCase(fieldname)) {
				columnid = "";
			} else {

				columnid = " as " + columnid;
			}

			if ("".equals(control)) {
				selectstr = selectstr + "," + tablename + "." + fieldname
						+ columnid + " ";
				groupstr = groupstr + "," + tablename + "." + fieldname;
			} else {
				if ("".equals(columnid)) {
					columnid = " as " + fieldname;
				}
				selectstr = selectstr + "," + control + "(" + tablename + "."
						+ fieldname + ") " + columnid + " ";
			}

			sqlList.add("update gmdatastore set tablename='" + tablename
					+ "' where datasetid='" + datasetId
					+ "' and fieldcaption='" + columnCaption + "'");
		}

		String sqlStr = " select " + selectstr + " from "
				+ tablestr.replaceAll("'", "''") + "  &where& ";
		if ("1".equals(isgroup)) {
			sqlStr = sqlStr + " group by " + groupstr;
		}
		sqlMap.put("key", keyStr);
		return sqlStr;
	}
	
	/**
	 * @param control
	 * @return
	 */
	private String getcontrolsql(String control) {
		if ("取数".equals(control)) {
			return "";
		} else if ("汇总".equals(control)) {
			return "sum";
		} else if ("最大".equals(control)) {
			return "Max";
		} else if ("最小".equals(control)) {
			return "Min";
		} else if ("平均".equals(control)) {
			return "Avg";
		}
		return "";
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
