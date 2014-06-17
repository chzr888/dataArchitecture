package com.victop.tool.dataarchitecture.active;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.json.JsonHelper;
import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * "业务规则生效"
 * 
 * @author chenzr
 * 
 */
public class BusinessRulesActive extends Active {

	private Logger log = LoggerFactory.getLogger(BusinessRulesActive.class);

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {

		String dtsID = resourceid;
		String masterName = "";
		String batchtype = "";
		// 生成数据引用
		String isSaveRef = "1";
		PreparedStatement statement = null;

		try {

			// ---批号特殊处理
			if (!"".equals(masterName)) {

				// ---修改物理表
				statement = H2Conn
						.prepareStatement("select * from gmmdldts where dtsid = '"
								+ dtsID + "' ");
				ResultSet gmmdldts = statement.executeQuery();

				boolean finded = false;
				while (gmmdldts.next()) {
					String Physicaltable = PublicFunc.reStringByNull(gmmdldts
							.getString("Physicaltable"));
					if ("".equals(Physicaltable)) {
						upDateBySql("update gmmdldts set physicaltable='bs"
								+ System.currentTimeMillis()
								+ "' where dtsid='" + dtsID + "'", H2Conn);
					}
					finded = true;
				}
				gmmdldts.close();
				statement.close();

				if (!finded) {
					upDateBySql(
							"insert into  gmmdldts (physicaltable,dtsid,dtstype,businesstype) values('bs"
									+ System.currentTimeMillis() + "','"
									+ dtsID + "','业务规则','批次')", H2Conn);
				}

				upDateBySql(
						" delete from gmdtsdim where dtsid='" + dtsID + "'",
						H2Conn);

				upDateBySql(
						"insert into gmdtsdim(dtsid,dimno,dimname,dimprop)values('"
								+ dtsID + "','1','','货号')", H2Conn);
				upDateBySql(
						"insert into gmdtsdim(dtsid,dimno,dimname,dimprop)values('"
								+ dtsID + "','2','','" + masterName + "')",
						H2Conn);
				upDateBySql(
						"insert into gmdtsdim(dtsid,dimno,dimname,dimprop)values('"
								+ dtsID + "','3','','" + batchtype + "')",
						H2Conn);

				upDateBySql(" delete from gmdtsfollowprop where dtsid='"
						+ dtsID + "' and dimname<>'" + masterName + "'", H2Conn);

				String batchfield = "";
				statement = H2Conn
						.prepareStatement("select * from gmprop where propname='"
								+ batchtype + "'");
				ResultSet batchtypers = statement.executeQuery();
				while (batchtypers.next()) {
					batchfield = batchtypers.getString("propfield");
				}
				batchtypers.close();
				statement.close();

				upDateBySql(
						" delete from gmdtscol where dtsid='" + dtsID + "'",
						H2Conn);
				String batchcvfield = "";
				for (int i = 1; i <= 10; i++) {
					batchcvfield = batchfield + "cv" + i;
					upDateBySql(
							"insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
									+ "values('" + dtsID + "','" + batchcvfield
									+ "','" + batchcvfield + "','字符')", H2Conn);
				}

			} else {

				// ---修改物理表
				statement = H2Conn
						.prepareStatement("select * from gmmdldts where dtsid = '"
								+ dtsID
								+ "' and (Physicaltable='' or (nullif(Physicaltable,null)=null) or Physicaltable='未命名')");
				ResultSet gmmdldts = statement.executeQuery();
				while (gmmdldts.next()) {
					upDateBySql("update gmmdldts set physicaltable='bs"
							+ System.currentTimeMillis() + "' where dtsid='"
							+ dtsID + "'", H2Conn);
				}
				gmmdldts.close();
				statement.close();
			}

			// ---更新角度字段
			// upDateBySql(
			// "update gmdtsdim,gmprop  set  gmdtsdim.dimfield=gmprop.propfield "
			// + "where gmdtsdim.dtsid='" + dtsID
			// + "' and gmdtsdim.dimprop=gmprop.propname",
			// bizConn);
			String sqlString = "SELECT gmprop.propfield,gmprop.propname FROM gmdtsdim, gmprop"
					+ " WHERE gmdtsdim.dtsid = '"
					+ dtsID
					+ "' AND gmdtsdim.dimprop = gmprop.propname";

			ArrayList<String> list = new ArrayList<String>();

			Statement statement3 = H2Conn.createStatement();
			ResultSet rSet3 = statement3.executeQuery(sqlString);
			while (rSet3.next()) {
				list.add("update gmdtsdim  set  dimfield='"
						+ rSet3.getString("propfield") + "' " + "where dtsid='"
						+ dtsID + "' and dimprop='"
						+ rSet3.getString("propname") + "'");
				// upDateBySql("update gmdtsdim  set  gmdtsdim.dimfield='"+rSet3.getString("propfield")+"' "
				// +
				// "where gmdtsdim.dtsid='"+dtsID+"' and gmdtsdim.dimprop='"+rSet3.getString("propname")+"'",bizConn);
			}
			rSet3.close();
			statement3.close();

			for (int i = 0; i < list.size(); i++) {
				upDateBySql(list.get(i), H2Conn);
			}

			// ---更新数据项字段
			statement = H2Conn
					.prepareStatement("select * from gmdtscol where dtsid = '"
							+ dtsID + "'  order by columnfield desc");
			ResultSet gmdtscol = statement.executeQuery();
			int no = 1, curno = -1;
			String columnfield = "", columnid = "";
			while (gmdtscol.next()) {
				columnfield = PublicFunc.reStringByNull(gmdtscol
						.getString("columnfield"));
				columnid = PublicFunc.reStringByNull(gmdtscol
						.getString("columnid"));
				if ("".equals(columnfield)) {
					no = no + 1;
					upDateBySql("update gmdtscol set  columnfield='field" + no
							+ "'  where dtsid='" + dtsID + "' and columnid='"
							+ columnid + "'", H2Conn);
				} else {
					// --获取最大数值
					curno = -1;
					try {
						curno = Integer.valueOf(columnfield.replaceAll("field",
								""));
					} catch (Exception e) {

					}
					if (no < curno) {
						no = curno;
					}
				}

			}
			gmdtscol.close();
			statement.close();

			// --数据原表生效
			DtsActive dts = new DtsActive();
			dts.setConn(H2Conn, BizConn, ConfConn, memConn);
			dts.execute(params, resourceid, resourceGuid, schemaverGuid,
					connMap);

			// ---生成数据引用
			if (!"1".equals(isSaveRef)) {
				upDateBySql(
						"delete from gmdatareference where type='dts' and modelid='"
								+ dtsID + "' ", H2Conn);
			}

			List<String> sqlList = new ArrayList<String>();
			statement = H2Conn
					.prepareStatement("select * from gmdtsdim where dtsid='"
							+ dtsID + "'");
			ResultSet columnRs = statement.executeQuery();
			while (columnRs.next()) {
				String sqlStr = "select a.propname,a.propfield,b.columncaption,b.columnid from gmdtsfollowprop a "
						+ "join gmmastertablecolumn  b on a.dimname=b.mastername and a.propname=b.columncaption   "
						+ "where a.dtsid='"
						+ dtsID
						+ "' and dimname='"
						+ columnRs.getString("dimname") + "'";
				createDataRef("dts", dtsID, "1", columnRs.getString("dimprop"),
						columnRs.getString("dimfield"),
						columnRs.getString("dimname"), sqlStr, sqlList, H2Conn);
			}
			columnRs.close();
			statement.close();

			for (int i = 0; i < sqlList.size(); i++) {
				upDateBySql(sqlList.get(i), H2Conn);
			}

			// 外观设置
			upDateBySql(
					" delete from gmdtsview where dtsid='"
							+ dtsID
							+ "' "
							+ "and columncaption not in (select columncaption from  gmdtstablecolumn where dtsid='"
							+ dtsID + "')", H2Conn);

			upDateBySql(
					" insert into  gmdtsview(dtsid,columncaption,columnid) "
							+ "select dtsid,columncaption,columnid   "
							+ "from gmdtstablecolumn "
							+ " where  dtsid='"
							+ dtsID
							+ "' "
							+ "and columncaption not in (select columncaption from  gmdtsview where dtsid='"
							+ dtsID + "')", H2Conn);
			return dtsID + ":检查成功";

		} catch (Exception e) {
			log.error(e.getMessage());
			throw e;
		}
	}

	/**
	 * 插入数据引用数据
	 * 
	 * @param Type
	 *            类型
	 * @param modelid
	 *            数据模型
	 * @param name
	 *            数据集id
	 * @param columnCaption
	 *            触发字段中文
	 * @param columnid
	 *            触发字段
	 * @param mastername
	 *            主档
	 * @param followPropSql
	 *            编写触发字段跟随属性sql语句(第一列为属性名称，第二列为属性字段,第三列引用的字段中文，第四列引用的字段)
	 * @param sqlList
	 *            列表
	 * @param metaConn
	 *            元数据库
	 * @throws SQLException
	 */
	private void createDataRef(String type, String modelId, String name,
			String columnCaption, String columnid, String mastername,
			String followPropSql, List<String> sqlList, Connection metaConn)
			throws SQLException {

		PreparedStatement statement = null;

		statement = metaConn
				.prepareStatement("select 1 from gmdatareference where type='"
						+ type + "' and modelid='" + modelId + "' and name='"
						+ name + "' and  columncaption='" + columnCaption + "'");
		ResultSet findrs = statement.executeQuery();
		while (findrs.next()) {
			return;
		}
		findrs.close();
		statement.close();

		// ---参数
		List<List<Map<String, String>>> fieldparam = new ArrayList<List<Map<String, String>>>();
		// --返回字段
		List<Map<String, String>> fieldreturn = new ArrayList<Map<String, String>>();
		// // ---操作
		// Map<String, String> operation = new HashMap<String, String>();

		String propname = "", propfield = "", linkpropname, linkfield;

		// ---循环跟随属性
		if (!"".equals(followPropSql)) {
			statement = metaConn.prepareStatement(followPropSql);
			ResultSet followRs = statement.executeQuery();

			while (followRs.next()) {
				propname = followRs.getString(1);
				propfield = followRs.getString(2);
				linkpropname = followRs.getString(3);
				linkfield = followRs.getString(4);

				Map<String, String> returnmap = new HashMap<String, String>();
				returnmap.put("linkdata", linkfield);
				returnmap.put("linkdatacaption", linkpropname);
				returnmap.put("data", propfield);
				returnmap.put("datacaption", propname);
				fieldreturn.add(returnmap);
			}
			followRs.close();
			statement.close();
		}

		statement = metaConn
				.prepareStatement("select columncaption,columnid from  gmmaster a "
						+ "join  gmmastertablecolumn  b on a.mastername=b.mastername and a.masterprop=b.columncaption  "
						+ "where a.mastername='" + mastername + "'");
		ResultSet gmmaster = statement.executeQuery();
		while (gmmaster.next()) {

			linkfield = gmmaster.getString("columnid");
			linkpropname = gmmaster.getString("columncaption");

			Map<String, String> returnmap = new HashMap<String, String>();
			returnmap.put("linkdata", linkfield);
			returnmap.put("linkdatacaption", linkpropname);
			returnmap.put("data", columnid);
			returnmap.put("datacaption", columnCaption);
			fieldreturn.add(returnmap);

			List<Map<String, String>> temp = new ArrayList<Map<String, String>>();
			Map<String, String> datamap = new HashMap<String, String>();
			// -----
			datamap.put("logiccaption", "模糊匹配");
			datamap.put("logic", "like");
			datamap.put("dataid", "1");
			datamap.put("datacaption", columnCaption);
			datamap.put("data", columnid);
			datamap.put("linkdatacaption", linkpropname);
			datamap.put("linkdata", linkfield);

			temp.add(datamap);
			fieldparam.add(temp);
		}
		gmmaster.close();
		statement.close();

		String operation = "{;top;operation;top;:;top;1;top;,;top;isdata;top;:;top;1;top;,;top;defaultdata;top;:[]}";
		String fieldreturnStr = JsonHelper.writeJson(fieldreturn).replaceAll(
				"\"", ";top;");
		String fieldparamStr = JsonHelper.writeJson(fieldparam).replaceAll(
				"\"", ";top;");

		sqlList.add("insert into gmdatareference(Type,modelid,name,columnid,columncaption,opentype,fieldparam,fieldreturn,operation,bsname)"
				+ "values('"
				+ type
				+ "','"
				+ modelId
				+ "','"
				+ name
				+ "','"
				+ columnid
				+ "','"
				+ columnCaption
				+ "','3','"
				+ fieldparamStr
				+ "','"
				+ fieldreturnStr
				+ "','"
				+ operation
				+ "','"
				+ mastername + "查询')");
	}

	private void upDateBySql(String sql, Connection conn) throws SQLException {
		log.info(sql);
		PreparedStatement preparedStatement = conn.prepareStatement(sql);
		preparedStatement.execute();
		preparedStatement.close();
	}

}
