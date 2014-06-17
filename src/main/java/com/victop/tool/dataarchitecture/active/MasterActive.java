package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.rowset.CachedRowSetImpl;
import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * "主档生效"
 * 
 * @author chenzr
 * 
 */
public class MasterActive extends Active {

	private Logger log = LoggerFactory.getLogger(MasterActive.class);

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		String mastername = resourceid;
		Statement statement = null;

		try {

			// 去空格
			upDateBySql(
					"UPDATE gmmastertablecolumn SET columncaption = trim(columncaption),columnid = trim(columnid) WHERE mastername = '"
							+ mastername
							+ "' AND columncaption = columncaption ", H2Conn);

			// --检查主档

			String sql = "select a.mastername,a.mastertablename,a.masterprop,a.masterpropname,a.ispropclass,a.isgroupmodify"
					+ ",a.grouppropmaster,a.isdefault,a.status,a.isoutside,a.istree,b.masterprop as propmastername  "
					+ "from gmmaster a left outer join gmmaster b on a.grouppropmaster=b.mastername  where a.mastername='"
					+ mastername + "'";

			ResultSet gmmaster = getResultSetBySql(sql, H2Conn);
			statement = gmmaster.getStatement();

			String reMsgString = MasterActiveCheck(gmmaster, mastername, H2Conn);
			if (!"".equals(reMsgString)) {
				throw new Exception(reMsgString);
			}

			// 维护后台表数据-------------------------------------------------------------------------------------------
			reMsgString = MasterActiveDealData(gmmaster, mastername, H2Conn);
			if (!"".equals(reMsgString)) {
				throw new Exception(reMsgString);
			}
			gmmaster.close();
			statement.close();

			// ----修改主档状态
			// upDateBySql(
			// "update gmmaster set status=1  where mastername='"
			// + mastername + "'", bizConn);

			PreparedStatement pst = H2Conn
					.prepareStatement("UPDATE gmmaster SET status = ? , stamp = ? WHERE mastername = ?");
			pst.setInt(1, 1);
			pst.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			pst.setString(3, mastername);
			pst.execute();
			pst.close();

			// 创造日志
			String logdts = createLog(mastername, H2Conn);
			if (!"".equals(logdts)) {
				DtsActive dts = new DtsActive();
				dts.setConn(H2Conn, BizConn, ConfConn, memConn);
				dts.execute(params, logdts, resourceGuid, schemaverGuid,
						connMap);
			}
		} catch (Exception e) {
			throw e;
		}

		return mastername + "生效成功";
	}

	/**
	 * 检查主档设置
	 * 
	 * @param rs
	 * @return
	 * @throws Exception
	 */
	private String MasterActiveCheck(ResultSet gmmaster, String mastername,
			Connection bizConn) throws Exception {
		boolean iscur = false;
		String MasterTableName = "", MasterProp = "", MasterPropName = "", grouppropmaster = "", istree = "", status = "", isOutSide = "", isgroupmodify = "", masterfield = "", masterCaption = "", groupField = "";

		try {
			while (gmmaster.next()) {
				MasterTableName = PublicFunc.reStringByNull(gmmaster
						.getString("MasterTableName"));
				MasterProp = PublicFunc.reStringByNull(gmmaster
						.getString("MasterProp"));
				MasterPropName = PublicFunc.reStringByNull(gmmaster
						.getString("MasterPropName"));
				grouppropmaster = PublicFunc.reStringByNull(gmmaster
						.getString("grouppropmaster"));
				istree = PublicFunc
						.reStringByNull(gmmaster.getString("istree"));
				isOutSide = PublicFunc.reStringByNull(gmmaster
						.getString("isOutSide"));
				status = PublicFunc
						.reStringByNull(gmmaster.getString("status"));
				isgroupmodify = PublicFunc.reStringByNull(gmmaster
						.getString("isgroupmodify"));
				iscur = true;
			}

			if (!iscur) {
				return "请保存主档后在生效!";
			}
			// if ("1".equals(isOutSide)) {
			// return "外部数据需要在暂时需要在表里设置!";
			// }
			if ("".equals(MasterTableName)) {
				return "请设置主档物理存储表名!";
			}
			if ("".equals(MasterProp)) {
				return "请设置主属性!";
			}
//			String checkString = checkMasterProp(mastername, MasterProp,
//					MasterPropName, bizConn);
//			if (!"".equals(checkString)) {
//				return checkString;
//			}

			if ("1".equals(isgroupmodify)) {
				iscur = false;
				String checkMsg = "";
				List<Integer> groupidList = new ArrayList<Integer>();
				PreparedStatement pst = null;
				pst = bizConn
						.prepareStatement("select propgroupid from gmpgroup");
				ResultSet pgroup = pst.executeQuery();
				while (pgroup.next()) {
					groupidList.add(pgroup.getInt("propgroupid"));
				}
				pgroup.close();
				pst.close();

				pst = bizConn
						.prepareStatement("select * from GMmasterpgroup where mastername='"
								+ mastername + "'");
				ResultSet GMmasterpgroup = pst.executeQuery();
				log.info("检查维护组设置......");
				while (GMmasterpgroup.next()) {
					iscur = true;

					if (!groupidList.contains(GMmasterpgroup
							.getInt("propgroupid"))) {
						checkMsg = "维护组编号："
								+ GMmasterpgroup.getInt("propgroupid") + "【"
								+ GMmasterpgroup.getString("propgroupname")
								+ "】不存在于维护组设置。";
					}
				}
				GMmasterpgroup.close();
				pst.close();
//				return "";

				if (!iscur) {
					return "该主档设置分组管理，但没有设置分组，请检查!";
				}
				if (!"".equals(checkMsg)) {
					return checkMsg;
				}
			}
			return "";
		} catch (Exception e) {
			throw e;
		}

	}

	private String checkMasterProp(String mastername, String masterprop,
			String masterpropname, Connection bizConn) throws SQLException {
		String msg = mastername;
		if (!checkExist(masterprop, bizConn)) {
			msg += "\n主属性[" + masterprop + "]不存在与于属性设置，请先设置！";
		}
		if (!"".equals(masterpropname) && !checkExist(masterpropname, bizConn)) {
			msg += "\n主属性名称[" + masterpropname + "]不存在与于属性设置，请先设置！";
		}

		ResultSet prop = getResultSetBySql(
				"select propname,propfield from gmmasterprop where mastername = '"
						+ mastername + "'", bizConn);

		while (prop.next()) {
			String propname = PublicFunc.reStringByNull(prop
					.getString("propname"));
			String propfield = PublicFunc.reStringByNull(prop
					.getString("propfield"));
			if ("".equals(propname) && "".equals(propfield)) {
				msg += "\n存在空的属性！";
			}
			if ("".equals(propname)) {
				msg += "\n属性字段[" + propfield + "]没有属性名称";
			}
			if ("".equals(propfield)) {
				msg += "\n属性[" + propname + "]没有属性字段";
			}
			if (!checkExist(propname, bizConn)) {
				msg += "\n属性[" + propname + "]不存在与于属性设置，请先设置！";
			}
			if (masterprop.equals(propname)) {
				msg += "\n属性[" + propname + "]与主属性定义重复！";
			}
			if (masterpropname.equals(propname)) {
				msg += "\n属性[" + propname + "]与主属性名称定义重复！";
			}
		}

		prop.close();
		if (!mastername.equals(msg)) {
			return msg;
		} else
			return "";
	}

	private boolean checkExist(String propname, Connection bizConn)
			throws SQLException {
		PreparedStatement pst = bizConn
				.prepareStatement("select 1 from gmprop where propname = '"
						+ propname + "'");
		ResultSet set = pst.executeQuery();

		while (set.next()) {
			set.close();
			return true;
		}

		set.close();
		pst.close();
		return false;
	}

	// --维护后台表结构
	private String MasterActiveDealData(ResultSet gmmaster, String mastername,
			Connection metaConn) throws SQLException {

		List<String> TableColumn = new ArrayList<String>();
		String MasterTableName = "", isgroupmodify = "", MasterProp = "", MasterPropName = "", grouppropmaster = "", istree = "", status = "", isOutSide = "", ispropclass = "", masterfield = "", masterCaption = "", groupField = "", masterfielddatatype = "", masterCaptiondatatype = "", groupFielddatatype = "";

		gmmaster.beforeFirst();
		while (gmmaster.next()) {
			MasterTableName = PublicFunc.reStringByNull(gmmaster
					.getString("MasterTableName"));
			MasterProp = PublicFunc.reStringByNull(gmmaster
					.getString("MasterProp"));
			MasterPropName = PublicFunc.reStringByNull(gmmaster
					.getString("MasterPropName"));
			grouppropmaster = PublicFunc.reStringByNull(gmmaster
					.getString("propmastername"));
			istree = PublicFunc.reStringByNull(gmmaster.getString("istree"));
			ispropclass = PublicFunc.reStringByNull(gmmaster
					.getString("ispropclass"));
			isgroupmodify = PublicFunc.reStringByNull(gmmaster
					.getString("isgroupmodify"));
		}

		TableColumn.add("delete from gmmastertablecolumn where mastername='"
				+ mastername + "'");

		int no = -7;
		String propName = "";
		PreparedStatement pst = metaConn
				.prepareStatement("select * from gmprop  where propname in ('"
						+ MasterProp + "','" + MasterPropName + "','"
						+ grouppropmaster + "')");
		ResultSet gmprop = pst.executeQuery();

		while (gmprop.next()) {
			propName = gmprop.getString("propName");
			if (propName.equalsIgnoreCase(MasterProp)) {
				masterfield = gmprop.getString("propfield");
				masterfielddatatype = gmprop.getString("datatype");
			} else if (propName.equalsIgnoreCase(MasterPropName)) {
				masterCaption = gmprop.getString("propfield");
				masterCaptiondatatype = gmprop.getString("datatype");
			} else if (propName.equalsIgnoreCase(grouppropmaster)) {
				groupField = gmprop.getString("propfield");
				groupFielddatatype = gmprop.getString("datatype");
			}
		}
		gmprop.close();
		pst.close();

		if (!"".equals(masterfield)) {
			boolean b = false;
			b = checkField(metaConn, "select 1 from GMMasterProp" +
					" where MasterName = '"+mastername+"' AND propname = '"+MasterProp+"'");
			upDateBySql(metaConn, "UPDATE gmmastertablecolumn " +
					"SET iskey = 1 , keyinput = 1 , mainprop = 'mastervalue'" +
					" WHERE MasterName = '"+mastername+"' AND columncaption = '"+MasterProp+"'");
			if(!b){
				TableColumn.add(InsertgmmasterTableColumnSql(mastername,
						MasterProp, masterfield.toUpperCase(), "mastervalue",
						masterfielddatatype, Integer.toString(no), "1", "1"));
				no = no + 1;
			}
		}
		if (!"".equals(masterCaption)) {
			boolean b = false;
			b = checkField(metaConn, "select 1 from GMMasterProp" +
					" where MasterName = '"+mastername+"' AND propname = '"+MasterPropName+"'");
			upDateBySql(metaConn, "UPDATE gmmastertablecolumn " +
					"SET mainprop = 'mastercaption'" +
					" WHERE MasterName = '"+mastername+"' AND columncaption = '"+MasterPropName+"'");
			if(!b){
				TableColumn.add(InsertgmmasterTableColumnSql(mastername,
						MasterPropName, masterCaption.toUpperCase(),
						"mastercaption", masterCaptiondatatype,
						Integer.toString(no), "0", "0"));
				no = no + 1;
			}
		}
		if (!"".equals(groupField)) {
			boolean b = false;
			b = checkField(metaConn, "select 1 from GMMasterProp" +
					" where MasterName = '"+mastername+"' AND propname = '"+grouppropmaster+"'");
			upDateBySql(metaConn, "UPDATE gmmastertablecolumn " +
					"SET mainprop = 'propclassid'" +
					" WHERE MasterName = '"+mastername+"' AND columncaption = '"+grouppropmaster+"'");
			if(!b){
				TableColumn.add(InsertgmmasterTableColumnSql(mastername,
						grouppropmaster, groupField.toUpperCase(), "propclassid",
						groupFielddatatype, Integer.toString(no), "0", "0"));
				no = no + 1;
			}
		}
		
		if(!checkField(metaConn, "select 1 from GMMasterProp" +
				" where MasterName = '"+mastername+"' AND propname = '生效'")){
			TableColumn.add(InsertgmmasterTableColumnSql(mastername, "生效",
					"ACTIVED", "actived", "整型", Integer.toString(no), "0", "0"));
			no = no + 1;
		}
		if(!checkField(metaConn, "select 1 from GMMasterProp" +
				" where MasterName = '"+mastername+"' AND propname = '本节点'")){
			TableColumn.add(InsertgmmasterTableColumnSql(mastername, "本节点", "ID",
					"ID", "字符", Integer.toString(no), "0", "0"));
			no = no + 1;
		}

		if ("1".equals(istree)) {
			if(!checkField(metaConn, "select 1 from GMMasterProp" +
					" where MasterName = '"+mastername+"' AND propname = '上级节点'")){
				TableColumn.add(InsertgmmasterTableColumnSql(mastername, "上级节点",
						"PARENTID", "ParentID", "字符", Integer.toString(no), "0",
						"0"));
				no = no + 1;
			}
		}

		if(!checkField(metaConn, "select 1 from GMMasterProp" +
				" where MasterName = '"+mastername+"' AND propname = '修改人'")){
			TableColumn.add(InsertgmmasterTableColumnSql(mastername, "修改人",
					"EDITMAN", "editman", "字符", Integer.toString(no), "0", "0"));
			no = no + 1;
		}


		if(!checkField(metaConn, "select 1 from GMMasterProp" +
				" where MasterName = '"+mastername+"' AND propname = '修改时间'")){
			TableColumn.add(InsertgmmasterTableColumnSql(mastername, "修改时间",
					"EDITDATE", "editdate", "日期", Integer.toString(no), "0", "0"));
			no = no + 1;
		}


		TableColumn
				.add("insert into gmmastertablecolumn(mastername,columncaption,columnid,"
						+ "mainprop,datatype,propgroupid,propclassid,no,keyinput) "
						+ "select '"
						+ mastername
						+ "',a.propname,upper(a.propfield),'',b.datatype,a.propgroupid,a.propclassid,a.no,a.keyinput"
						+ " from  GMMasterProp  a join gmprop b on a.propname=b.propname where mastername='"
						+ mastername + "' order by no");
		
		if (!"".equals(masterfield)) {
			TableColumn
			.add("UPDATE gmmastertablecolumn " +
					"SET iskey = 1 , keyinput = 1 , mainprop = 'mastervalue'" +
					" WHERE MasterName = '"+mastername+"' AND columncaption = '"+MasterProp+"'");
		}
		if (!"".equals(masterCaption)) {
			TableColumn
			.add( "UPDATE gmmastertablecolumn " +
					"SET mainprop = 'mastercaption'" +
					" WHERE MasterName = '"+mastername+"' AND columncaption = '"+MasterPropName+"'");
		}
		if (!"".equals(groupField)) {
			TableColumn
			.add( "UPDATE gmmastertablecolumn " +
					"SET mainprop = 'propclassid'" +
					" WHERE MasterName = '"+mastername+"' AND columncaption = '"+grouppropmaster+"'");
		}

		TableColumn.add( "UPDATE gmmastertablecolumn " +
				"SET mainprop = 'actived'" +
				" WHERE MasterName = '"+mastername+"' AND columncaption = '生效'");
		
		
		TableColumn.add( "UPDATE gmmastertablecolumn " +
				"SET mainprop = 'ID'" +
				" WHERE MasterName = '"+mastername+"' AND columncaption = '本节点'");
		
		TableColumn.add( "UPDATE gmmastertablecolumn " +
				"SET mainprop = 'ParentID'" +
				" WHERE MasterName = '"+mastername+"' AND columncaption = '上级节点'");
		
		TableColumn.add( "UPDATE gmmastertablecolumn " +
				"SET mainprop = 'editman'" +
				" WHERE MasterName = '"+mastername+"' AND columncaption = '修改人'");
		
		TableColumn.add( "UPDATE gmmastertablecolumn " +
				"SET mainprop = 'editdate'" +
				" WHERE MasterName = '"+mastername+"' AND columncaption = '修改时间'");

		// ----数据字典
		TableColumn.add("delete from gmmdlentity where EntityID='"
				+ MasterTableName + "'");

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String data = df.format(new Date());

		TableColumn
				.add("insert into gmmdlentity(EntityID,EntityCaption,keyfields,stamp)values('"
						+ MasterTableName
						+ "','"
						+ mastername
						+ "','"
						+ masterfield + "','" + data + "')");

		String error = "";

		if ("".equals(isgroupmodify)) {
			TableColumn.add("delete from gmmasterpgroup where mastername='"
					+ mastername + "'");
			TableColumn
					.add("insert into gmmasterpgroup (mastername,propgroupid,propgroupname) values('"
							+ mastername + "','1','默认组')");
		}

		// ---删除没有的，新增有的 gmmasterview
		TableColumn
				.add("delete from gmmasterview where mastername='"
						+ mastername
						+ "' and  propname not in (select columncaption from gmmastertablecolumn where mastername='"
						+ mastername + "')");

		TableColumn
				.add("insert into  gmmasterview (mastername,propname)"
						+ "select mastername,columncaption from "
						+ "gmmastertablecolumn where mastername='"
						+ mastername
						+ "' and  columncaption not in (select propname from gmmasterview where mastername='"
						+ mastername + "')");

		TableColumn.add("update gmmasterprop set actived=1 where mastername='"
				+ mastername + "'");
		TableColumn
				.add("update gmmasterpgroup set actived=1 where mastername='"
						+ mastername + "'");
		TableColumn
				.add("update gmmasterview set listshow = 1 where mastername = '"
						+ mastername
						+ "'"
						+ " and propname not in('本节点','父节点','修改人','修改时间','上级节点')");

		for (int i = 0; i < TableColumn.size(); i++) {
			upDateBySql(TableColumn.get(i), metaConn);
		}

		return error;
	}

	/**
	 * 插入主档后台表
	 * 
	 * @param mastername
	 * @param columncaption
	 * @param columnid
	 * @param mainprop
	 * @param datatype
	 * @param no
	 * @return
	 */
	private String InsertgmmasterTableColumnSql(String mastername,
			String columncaption, String columnid, String mainprop,
			String datatype, String no, String keyinput, String iskey) {
		String sql = "insert into gmmastertablecolumn(mastername,columncaption,columnid,"
				+ "mainprop,datatype,propgroupid,propclassid,no,keyinput,iskey)values('"
				+ mastername
				+ "','"
				+ columncaption
				+ "','"
				+ columnid
				+ "','"
				+ mainprop
				+ "','"
				+ datatype
				+ "','1','1',"
				+ no
				+ ","
				+ keyinput + "," + iskey + ")";

		return sql;
	}

	private String createLog(String mastername, Connection bizConn)
			throws Exception {
		String logtablename = "";
		String physicaltable = "";
		int islog = 0;
		PreparedStatement pst = bizConn
				.prepareStatement("select MasterTableName,islog from gmmaster where mastername = '"
						+ mastername + "'");
		ResultSet gmmdldts = pst.executeQuery();
		while (gmmdldts.next()) {
			islog = gmmdldts.getInt("islog");
			physicaltable = gmmdldts.getString("MasterTableName");
		}
		gmmdldts.close();
		pst.close();
		if (islog == 1) {
			log.info("插入日志表的设置");
			logtablename = mastername + "日志";
			boolean exist = false;
			pst = bizConn
					.prepareStatement("select 1 from gmmdldts where dtsid = '"
							+ logtablename + "'");
			gmmdldts = pst.executeQuery();
			while (gmmdldts.next()) {
				exist = true;
			}
			// 需要生成日志表并未存在日志表
			if (!exist) {
				physicaltable = physicaltable + "_log";
				log.info(logtablename);
				upDateBySql(
						"insert into gmmdldts (dtsid,dtstype,Physicaltable,businesstype) "
								+ " values ('" + logtablename + "','外部数据','"
								+ physicaltable + "','日志')", bizConn);
				upDateBySql(
						"insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) "
								+ " values ('" + logtablename
								+ "','唯一号','id','字符','1')", bizConn);
				upDateBySql(
						"insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) "
								+ " values ('" + logtablename
								+ "','修改人','modifyname','字符','0')", bizConn);
				upDateBySql(
						"insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) "
								+ " values ('" + logtablename
								+ "','修改时间','modifytime','日期','0')", bizConn);
				upDateBySql(
						"insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) "
								+ " values ('" + logtablename
								+ "','修改数据','modifydata','longtext','0')",
						bizConn);
			} else {
				logtablename = "";
			}
		}
		return logtablename;
	}
	
	private boolean checkField(Connection conn,String sql) throws SQLException {
		ResultSet rs = null;
		try {
			rs = getResultSetBySql(conn, sql);
			if(rs.next()){
				return true;
			}
		} catch (SQLException e) {
			throw e;
		}finally{
			closeResultSet(rs);
		}
		return false;
	}

	private void upDateBySql(String sql, Connection conn) throws SQLException {
		log.info(sql);
		PreparedStatement preparedStatement = conn.prepareStatement(sql);
		preparedStatement.execute();
		preparedStatement.close();
	}

	private ResultSet getResultSetBySql(String sql, Connection conn)
			throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = stmt.executeQuery(sql);
		return rs;
	}

}
