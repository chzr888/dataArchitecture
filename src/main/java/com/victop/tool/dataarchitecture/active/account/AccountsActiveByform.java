package com.victop.tool.dataarchitecture.active.account;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * 功能号对应业务账务生效
 * @author chenzr
 *
 */
public class AccountsActiveByform extends Active {

	private Logger log = LoggerFactory.getLogger(AccountsActiveByform.class);
	
	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		log.info("收到前端参数:{}", params);

		String formid = resourceid;
		String modelid = "", datasetid = "", type = "";

		String errorStr = "";

		String Sql = "select modelid from GSformmodellay where formid='"
				+ formid + "'";
		log.debug("查找数据模型：{}", Sql);
		ResultSet ModelidDs = H2Conn.prepareStatement(Sql).executeQuery();
		while (ModelidDs.next()) {
			modelid = ModelidDs.getString("modelid");
			break;
		}
		ModelidDs.close();
		if ("".equals(modelid)) {
			return null;
//			throw new Exception("没有找到modelid");
		}
		ResultSet tempRS = getResultSetBySql(H2Conn, "SELECT DISTINCT accountsid FROM gmformaccount WHERE formid = '"+formid+"'");
		while (tempRS.next()) {
			String accountsid = tempRS.getString("accountsid");
			// --找出功能号对应业务账务的明细
			Sql = "select gmformaccount.datasetid,gmaccont.type  "
					+ "from gmformaccount join gmaccont on  gmformaccount.accountsid=gmaccont.accountsid  where formid="
					+ formid + " and gmformaccount.accountsid='" + accountsid + "'";
			log.debug("{}", Sql);
			ResultSet gmformaccount = H2Conn.prepareStatement(Sql).executeQuery();
			while (gmformaccount.next()) {
				datasetid = gmformaccount.getString("datasetid");
				type = PublicFunc.reStringByNull(gmformaccount.getString("type"));
				break;
			}
			gmformaccount.close();

			if ("".equals(datasetid)) {
				throw new Exception("没有找到datasetid");
			}

			// if ("".equals(type) || "正常".equals(type)) {
			String string = dealwithNormal(datasetid, modelid, formid, accountsid,
						H2Conn);
			logger.info(string);
//			} else {
//				return dealwithSpecial(datasetid, modelid, formid, accountsid,
//						bizConn);
//			}
		}
		closeResultSet(tempRS);
		return null;
	}

	/**
	 * 处理正常账务
	 * 
	 * @param datasetid
	 * @param modelid
	 * @param formid
	 * @param accountsid
	 * @param metaConn
	 * @return
	 * @throws Exception
	 */
	private String dealwithNormal(String datasetid, String modelid,
			String formid, String accountsid, Connection metaConn)
			throws Exception {
		String Sql = "";
		log.debug("明细数据集编号:" + datasetid);

		// --查找循环体的数据原表
		Sql = "select a.datasetid,b.dtsid,b.Physicaltable,a.masterfields,a.detailfields from gmmodeldataset a  "
				+ "join  gmmdldts b  on a.fdatasetname=b.dtsid where a.modelid='"
				+ modelid + "' order by datasetid  ";
		log.debug("{}", Sql);
		ResultSet getDatasetTable = metaConn.prepareStatement(Sql)
				.executeQuery();
		Map<String, String> dimTable = new HashMap<String, String>();
		Map<String, String> dimfield = new HashMap<String, String>();

		// ---查询出字段,主从关系关键数据集查询语句
		String fdatasetname = "", ftableName = "", joinStr = "", rootTable = "", masterfields = "", detailfields = "", ptable = "", tempdatasetid = "";
		boolean isfirst = true;
		while (getDatasetTable.next()) {
			tempdatasetid = getDatasetTable.getString("datasetid");
			masterfields = getDatasetTable.getString("masterfields");
			detailfields = getDatasetTable.getString("detailfields");
			String curTableName = "T" + tempdatasetid;

			if (isfirst) {
				isfirst = false;
				rootTable = curTableName;
				// getDatasetTable.getString("Physicaltable");
			}

			if ("".equals(joinStr)) {
				joinStr = curTableName;
			} else {
				String[] masterlist = masterfields.split(";");
				String[] detaillist = detailfields.split(";");

				String joinLink = "";
				for (int i = 0; i < masterlist.length; i++) {
					if (!"".equals(joinLink)) {
						joinLink = joinLink + " and ";
					}
					joinLink = joinLink + ptable + "." + masterlist[i] + "="
							+ curTableName + "." + detaillist[i];
				}

				joinStr = joinStr + " join " + curTableName + " on " + joinLink
						+ " ";
			}

			dimTable.put(getDatasetTable.getString("datasetid"), curTableName);
			if (datasetid.equals(getDatasetTable.getString("datasetid"))) {
				fdatasetname = getDatasetTable.getString("dtsid");
				ftableName = curTableName;
				break;
			}
			ptable = curTableName;
		}
		getDatasetTable.close();

		if ("".equals(fdatasetname)) {
			throw new Exception("找不到功能号的数据集编号：" + datasetid);
		}

		// --字段，检查绑定字段，以及整理字段
		Sql = "select distinct a.datasetid,a.formdimcol,c.columnid from gmformaccountdim a "
				+ "left join gmmodeldataset b on  a.datasetid=b.datasetid and b.modelid='"
				+ modelid
				+ "'  left  join gmdtstablecolumn c on b.fdatasetname=c.dtsid and a.formdimcol=c.columncaption  where a.formid='"
				+ formid + "' and a.accountsid='" + accountsid + "' ";
		log.debug("{}", Sql);
		ResultSet dimRs = metaConn.prepareStatement(Sql).executeQuery();
		String selectStr = "";
		while (dimRs.next()) {
			if (!"".equals(selectStr)) {
				selectStr = selectStr + ",";
			}
			String formfield = PublicFunc.reStringByNull(dimRs
					.getString("columnid"));
			String formcaption = PublicFunc.reStringByNull(dimRs
					.getString("formdimcol"));
			if ("".equals(formfield)) {
				throw new Exception("功能号字段：" + formcaption + ",找不到值");
			}

			selectStr = selectStr + dimTable.get(dimRs.getString("datasetid"))
					+ "." + formfield;

			dimfield.put(dimRs.getString("formdimcol"), dimRs
					.getString("columnid"));
		}
		dimRs.close();
		String errorStr = "";

		// ---获取明细账物理表
		Sql = "select Physicaltable from  gmmdldts where dtsid='" + accountsid
				+ "明细账'";
		log.debug("{}", Sql);
		ResultSet gmmdldts = metaConn.prepareStatement(Sql).executeQuery();
		String Physicaltable = "";
		while (gmmdldts.next()) {
			Physicaltable = gmmdldts.getString("Physicaltable");
		}
		gmmdldts.close();

		// 循环对应关系，构建insert，和查询数据sql
		List<String> sqlList = new ArrayList<String>();
		Sql = "select b.columnid as formfield,d.propdatafield as accountfield, a.accountscol "
				+ "from gmformaccountcol a LEFT join gmdtstablecolumn b on a.formcol=b.columncaption and b.dtsid='"
				+ fdatasetname
				+ "' "
				+ "LEFT join gmdtstablecolumn c on a.accountscol=c.columncaption and c.dtsid='"
				+ accountsid
				+ "明细账' "
				+ "LEFT join gmdatatype d on c.datatype=d.datatype where a.formid='"
				+ formid + "' and a.accountsid='" + accountsid + "'";
		log.debug("{}", Sql);
		ResultSet gmAccontCol = metaConn.prepareStatement(Sql).executeQuery();
		String updateInSertSql, updateSelectSql, insertStr = "", insertValue = "";
		while (gmAccontCol.next()) {
            if(null==gmAccontCol.getString("accountfield")||"".equals(gmAccontCol.getString("accountfield"))){
            	throw new Exception("账务数据项【"+gmAccontCol.getString("accountscol")+"】绑定出错，请绑定明细账数据项！");
            }
            if(null==gmAccontCol.getString("formfield")||"".equals(gmAccontCol.getString("formfield"))){
            	throw new Exception("账务数据项【"+gmAccontCol.getString("accountscol")+"】绑定的功能数据项的字段不存在！");
            }
			insertStr = "";
			insertValue = "";
			Sql = "select a.formdimcol,b.columnid from gmformaccountdim a  left join gmdtstablecolumn b on a.accountsdimcol=b.columncaption and b.dtsid='"
					+ accountsid
					+ "明细账'   where a.formid='"
					+ formid
					+ "' and a.accountsid='"
					+ accountsid
					+ "' and a.accountscol='"
					+ gmAccontCol.getString("accountscol") + "' ";
			log.debug("{}", Sql);
			ResultSet coldimrs = metaConn.prepareStatement(Sql).executeQuery();

			while (coldimrs.next()) {
				if (!"".equals(insertStr)) {
					insertStr = insertStr + ",";
					insertValue = insertValue + ",";
				}

				errorStr = "账务字段：" + coldimrs.getString("formdimcol")
						+ ",对应数据项找不到";
				if ("".equals(PublicFunc.reStringByNull(coldimrs
						.getString("columnid")).toLowerCase())) {
					errorStr = "账务字段：" + coldimrs.getString("formdimcol")
							+ "不存在，请检查";
					throw new Exception(errorStr);
				}
				// System.out.println(coldimrs.getString("formdimcol"));
				insertStr = insertStr + coldimrs.getString("columnid");
				insertValue = insertValue
						+ "'&"
						+ dimfield.get(coldimrs.getString("formdimcol"))
								.toLowerCase() + "&'";
			}
			coldimrs.close();

			updateInSertSql = "insert into " + Physicaltable + " (" + insertStr
					+ ",itemname,periodid,"
					+ gmAccontCol.getString("accountfield") + ") values("
					+ insertValue + ",'&itemname&','&periodid&','&"
					+ gmAccontCol.getString("formfield").toLowerCase() + "&')";

			sqlList.add("update gmformaccountcol set insertsql='"
					+ updateInSertSql.replaceAll("'", "''")
					+ "' where formid='" + formid + "' and accountsid='"
					+ accountsid + "' and accountscol='"
					+ gmAccontCol.getString("accountscol") + "'");

			selectStr = selectStr + "," + ftableName + "."
					+ gmAccontCol.getString("formfield");

		}
		gmAccontCol.close();

		updateSelectSql = "select " + selectStr + " from " + joinStr
				+ "  where " + rootTable + ".doccode='&doccode&'";

		String getdocdateStr = "select docdate from " + rootTable
				+ " where doccode='&doccode&'";

		Sql = "select b.mastertablename,b.mastername from gmaccontdim a join  gmmaster b on  a.mastername=b.mastername "
				+ "where accountsid='" + accountsid + "' and businesstype='期间'";
		log.debug("{}", Sql);
		ResultSet Adim = metaConn.prepareStatement(Sql).executeQuery();
		String periodidTable = "", periodidtype = "";
		while (Adim.next()) {
			periodidTable = Adim.getString("mastertablename");
			periodidtype = Adim.getString("mastername");
		}
		Adim.close();

		String getperiodStr = "select periodid from " + periodidTable
				+ " where begindate<='&docdate&' and enddate>='&docdate&' ";

		sqlList.add("update gmformaccount set selectstr='"
				+ updateSelectSql.replaceAll("'", "''") + "' ,getdocdateStr='"
				+ getdocdateStr.replaceAll("'", "''") + "',getperiodStr='"
				+ getperiodStr.replaceAll("'", "''")
				+ "', actived=1 where formid='" + formid + "' and accountsid='"
				+ accountsid + "'");

		for (int i = 0; i < sqlList.size(); i++) {
			this.upDateBySql(sqlList.get(i), metaConn);
		}

		return accountsid + ":检查成功";
	}

	/**
	 * 特殊业务账务对应功能号生效
	 * 
	 * @param datasetid
	 * @param modelid
	 * @param formid
	 * @param accountsid
	 * @param metaConn
	 * @return
	 * @throws Exception
	 */
	private String dealwithSpecial(String datasetid, String modelid,
			String formid, String accountsid, Connection metaConn)
			throws Exception {

		String Sql = "";
		log.debug("明细数据集编号:" + datasetid);

		// --查找循环体的数据原表
		Sql = "select a.datasetid,b.dtsid,b.Physicaltable,a.masterfields,a.detailfields from gmmodeldataset a  "
				+ "join  gmmdldts b  on a.fdatasetname=b.dtsid where a.modelid='"
				+ modelid + "' order by datasetid  ";
		log.debug("{}", Sql);
		ResultSet getDatasetTable = metaConn.prepareStatement(Sql)
				.executeQuery();
		Map<String, String> dimTable = new HashMap<String, String>();
		Map<String, String> dimfield = new HashMap<String, String>();

		// ---查询出字段,主从关系关键数据集查询语句
		String fdatasetname = "", ftableName = "", joinStr = "", rootTable = "", masterfields = "", detailfields = "", ptable = "", tempdatasetid = "";
		boolean isfirst = true;
		while (getDatasetTable.next()) {
			tempdatasetid = getDatasetTable.getString("datasetid");
			masterfields = getDatasetTable.getString("masterfields");
			detailfields = getDatasetTable.getString("detailfields");
			String curTableName = "T" + tempdatasetid;

			if (isfirst) {
				isfirst = false;
				rootTable = curTableName;
			}

			if ("".equals(joinStr)) {
				joinStr = curTableName;
			} else {
				String[] masterlist = masterfields.split(";");
				String[] detaillist = detailfields.split(";");

				String joinLink = "";
				for (int i = 0; i < masterlist.length; i++) {
					if (!"".equals(joinLink)) {
						joinLink = joinLink + " and ";
					}
					joinLink = joinLink + ptable + "." + masterlist[i] + "="
							+ curTableName + "." + detaillist[i];
				}

				joinStr = joinStr + " join " + curTableName + " on " + joinLink
						+ " ";
			}

			dimTable.put(getDatasetTable.getString("datasetid"), curTableName);
			if (datasetid.equals(getDatasetTable.getString("datasetid"))) {
				fdatasetname = getDatasetTable.getString("dtsid");
				ftableName = curTableName;
				break;
			}
			ptable = curTableName;
		}
		getDatasetTable.close();

		if ("".equals(fdatasetname)) {
			throw new Exception("找不到功能号的数据集编号：" + datasetid);
		}

		// --字段，检查绑定字段，以及整理字段
		Sql = "select distinct a.datasetid,a.formdimcol,c.columnid from gmformaccountdim a "
				+ "join gmmodeldataset b on  a.datasetid=b.datasetid and b.modelid='"
				+ modelid
				+ "' join gmdtstablecolumn c on b.fdatasetname=c.dtsid and a.formdimcol=c.columncaption  where a.formid='"
				+ formid + "' and a.accountsid='" + accountsid + "' ";
		log.debug("{}", Sql);
		ResultSet dimRs = metaConn.prepareStatement(Sql).executeQuery();
		String selectStr = "";
		while (dimRs.next()) {
			if (!"".equals(selectStr)) {
				selectStr = selectStr + ",";
			}
			String formfield = PublicFunc.reStringByNull(dimRs
					.getString("columnid")).toLowerCase();
			String formcaption = PublicFunc.reStringByNull(dimRs
					.getString("formdimcol"));
			if ("".equals(formfield)) {
				throw new Exception("功能号字段：" + formcaption + ",找不到值");
			}

			selectStr = selectStr + dimTable.get(dimRs.getString("datasetid"))
					+ "." + formfield;

			dimfield.put(dimRs.getString("formdimcol"), dimRs
					.getString("columnid"));
		}
		dimRs.close();
		String errorStr = "";

		// ---获取明细账物理表
		Sql = "select Physicaltable from  gmmdldts where dtsid='" + accountsid
				+ "特殊明细账'";
		log.debug("{}", Sql);
		ResultSet gmmdldts = metaConn.prepareStatement(Sql).executeQuery();
		String Physicaltable = "";
		while (gmmdldts.next()) {
			Physicaltable = gmmdldts.getString("Physicaltable");
		}
		gmmdldts.close();

		// 循环对应关系，构建insert，和查询数据sql
		List<String> sqlList = new ArrayList<String>();
		Sql = "select b.columnid as formfield,d.propdatafield as accountfield, a.accountscol,f.columnid as detialColumnid,f.direction  "
				+ " from gmformaccountcol a  left join  gmaccontcol   f  "
				+ " on  a.accountsid=f.accountsid and  a.accountscol=f.columnid and f.type='明细账'  "
				+ " LEFT join gmdtstablecolumn b on a.formcol=b.columncaption and b.dtsid='"
				+ fdatasetname
				+ "' "
				+ " LEFT join gmdtstablecolumn c on a.accountscol=c.columncaption and c.dtsid='"
				+ accountsid
				+ "特殊明细账' "
				+ " LEFT join gmdatatype d on c.datatype=d.datatype where a.formid='"
				+ formid + "' and a.accountsid='" + accountsid + "'";
		log.debug("{}", Sql);
		ResultSet gmAccontCol = metaConn.prepareStatement(Sql).executeQuery();
		String updateInSertSql, updateSelectSql, selectSql = "", insertStr = "", updateselect, updatewhere = "", insertValue = "", detialColumnid = "", direction = "", orderbyStr = "", whereStr = "";
		while (gmAccontCol.next()) {
			detialColumnid = PublicFunc.reStringByNull(gmAccontCol
					.getString("detialColumnid"));
			direction = PublicFunc.reStringByNull(gmAccontCol
					.getString("direction"));

			if ("".equals(detialColumnid)) {
				throw new Exception("绑定账务中数据项："
						+ PublicFunc.reStringByNull(gmAccontCol
								.getString("accountscol"))
						+ ",没有在账务定义中找到对应的数据项，请检查！");
			}
			insertStr = "";
			insertValue = "";
			orderbyStr = "";
			whereStr = "";
			updateselect = "";
			updatewhere = "";
			Sql = "select a.formdimcol,b.columnid,f.orderstr  from gmformaccountdim a   "
					+ " left join gmaccontspecial f on  a.accountsid=f.accountsid and a.accountsdimcol=f.propname  "
					+ " left join gmdtstablecolumn b on a.accountsdimcol=b.columncaption and b.dtsid='"
					+ accountsid
					+ "特殊明细账'   where a.formid='"
					+ formid
					+ "' and a.accountsid='"
					+ accountsid
					+ "' and a.accountscol='"
					+ gmAccontCol.getString("accountscol") + "' ";
			log.debug("{}", Sql);
			ResultSet coldimrs = metaConn.prepareStatement(Sql).executeQuery();
			while (coldimrs.next()) {

				
				String orderstr = PublicFunc.reStringByNull(coldimrs
						.getString("orderstr"));

				updateselect = PublicFunc.reConnectString(updateselect,
						coldimrs.getString("columnid"), ",");
				
				log.debug("财务字段:"+coldimrs.getString("formdimcol")+",功能号字段:"+dimfield.get(coldimrs.getString("formdimcol")));
				
				updatewhere = PublicFunc.reConnectString(updatewhere, coldimrs
						.getString("columnid")
						+ "="
						+ "'&"
						+ dimfield.get(coldimrs.getString("formdimcol"))
								.toLowerCase() + "&' ", " and ");

				if (!"".equals(orderstr)) {
					// --
					if ("减".equals(direction)) {
						if ("先进先出".equals(orderstr)) {
							orderstr = coldimrs.getString("columnid")
									+ "  ase ";
						} else if ("后进先出".equals(orderstr)) {
							orderstr = coldimrs.getString("columnid")
									+ "  desc ";
						}
						orderbyStr = PublicFunc.reConnectString(orderbyStr,
								orderstr, ",");

						continue;
					}
				}

				if (!"".equals(insertStr)) {
					insertStr = insertStr + ",";
					insertValue = insertValue + ",";
					whereStr = whereStr + " and ";
				}

				errorStr = "账务字段：" + coldimrs.getString("formdimcol")
						+ ",对应数据项找不到";
				if ("".equals(PublicFunc.reStringByNull(coldimrs
						.getString("columnid")))) {
					errorStr = "账务字段：" + coldimrs.getString("formdimcol")
							+ "不存在，请检查";
					throw new Exception(errorStr);
				}
				// System.out.println(coldimrs.getString("formdimcol"));
				insertStr = insertStr + coldimrs.getString("columnid");
				insertValue = insertValue
						+ "'&"
						+ dimfield.get(coldimrs.getString("formdimcol"))
								.toLowerCase() + "&'";
				whereStr = whereStr
						+ coldimrs.getString("columnid")
						+ "="
						+ "'&"
						+ dimfield.get(coldimrs.getString("formdimcol"))
								.toLowerCase() + "&' ";
			}
			coldimrs.close();

			updateInSertSql = "insert into " + Physicaltable + " (" + insertStr
					+ ",itemname,periodid,backfilling,direction,"
					+ gmAccontCol.getString("accountfield") + ") values("
					+ insertValue
					+ ",'&itemname&','&periodid&',0,'&direction&','&"
					+ gmAccontCol.getString("formfield").toLowerCase() + "&')";

			sqlList.add("update gmformaccountcol set insertsql='"
					+ updateInSertSql.replaceAll("'", "''")
					+ "' where formid='" + formid + "' and accountsid='"
					+ accountsid + "' and accountscol='"
					+ gmAccontCol.getString("accountscol") + "'");

			if ("减".equals(direction)) {
				selectSql = "select &top& "
						+ gmAccontCol.getString("accountfield") + ",itemname,"
						+ updateselect + " from " + Physicaltable
						+ " where   direction='加' and ("
						+ gmAccontCol.getString("accountfield") + "-"
						+ "backfilling>0) and " + whereStr + " order by "
						+ orderbyStr + "  &limit&  ";
				String updateSql = "update "
						+ Physicaltable
						+ " set backfilling=backfilling+'&backfilling&'  where "
						+ updatewhere
						+ " and itemname='&itemname&' and direction='加' and ("
						+ gmAccontCol.getString("accountfield") + "-"
						+ "backfilling+'&backfilling&')>=0  ";
				sqlList.add("update gmformaccountcol set selectsql='"
						+ selectSql.replaceAll("'", "''") + "',updateSql='"
						+ updateSql.replaceAll("'", "''") + "' where formid='"
						+ formid + "' and accountsid='" + accountsid
						+ "' and accountscol='"
						+ gmAccontCol.getString("accountscol") + "'");
			}
			selectStr = selectStr + "," + ftableName + "."
					+ gmAccontCol.getString("formfield");

		}
		gmAccontCol.close();

		updateSelectSql = "select " + selectStr + " from " + joinStr
				+ "  where " + rootTable + ".doccode='&doccode&'";

		String getdocdateStr = "select docdate from " + rootTable
				+ " where doccode='&doccode&'";

		Sql = "select b.mastertablename,b.mastername from gmaccontdim a join  gmmaster b on  a.mastername=b.mastername "
				+ "where accountsid='" + accountsid + "' and businesstype='期间'";
		log.debug("{}", Sql);
		ResultSet Adim = metaConn.prepareStatement(Sql).executeQuery();
		String periodidTable = "", periodidtype = "";
		while (Adim.next()) {
			periodidTable = Adim.getString("mastertablename");
			periodidtype = Adim.getString("mastername");
		}
		Adim.close();

		String getperiodStr = "select periodid from " + periodidTable
				+ " where begindate<='&docdate&' and enddate>='&docdate&'";

		sqlList.add("update gmformaccount set selectstr='"
				+ updateSelectSql.replaceAll("'", "''") + "' ,getdocdateStr='"
				+ getdocdateStr.replaceAll("'", "''") + "',getperiodStr='"
				+ getperiodStr.replaceAll("'", "''")
				+ "', actived=1 where formid='" + formid + "' and accountsid='"
				+ accountsid + "'");

		for (int i = 0; i < sqlList.size(); i++) {
			this.upDateBySql(sqlList.get(i), metaConn);
		}

		return accountsid + ":检查成功";
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
