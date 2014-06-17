package com.victop.tool.dataarchitecture.active.bsrelation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * 业务关联生效
 */
public class BsRelationActived extends Active {
	
    private Logger log = LoggerFactory.getLogger(BsRelationActived.class);

	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
	   	
		   // --业务查询
	       String bsrelationname = resourceid;
	       List<String> sqlList = new ArrayList<String>();
	       try {
	           log.info("业务关联" + bsrelationname + "生效开始。");
	           PreparedStatement pst = null;

	           pst = H2Conn.prepareStatement("select a.type,a.formid,a.itemname,a.datasetid,a.direction,b.physicaltable  from gmbsrelationbind a join gmmdldts b on a.bsrelationname=b.dtsid"
	                   + " where a.bsrelationname='"
	                   + bsrelationname + "'");

	           ResultSet gmbsrelationbind = pst.executeQuery();

	           while (gmbsrelationbind.next()) {
	               String type = gmbsrelationbind.getString("type");
	               String formid = gmbsrelationbind.getString("formid");
	               String itemname = gmbsrelationbind.getString("itemname");
	               String datasetid = gmbsrelationbind.getString("datasetid");
	               String physicaltable = gmbsrelationbind
	               .getString("physicaltable");
	               String direction = PublicFunc.reStringByNull(gmbsrelationbind
	                       .getString("direction"));

	               if ("负".equals(direction)) {
	                   direction = "-";
	               } else {
	                   direction = "+";
	               }

	               bsrelationByFormid(type, bsrelationname, direction, formid,
	                       datasetid, itemname, physicaltable, sqlList,H2Conn);
	           }
	           gmbsrelationbind.close();
	           pst.close();

	           for (int i = 0; i < sqlList.size(); i++) {
	               upDateBySql(sqlList.get(i), H2Conn);
	           }

	           log.info("状态");
	           upDateBySql(
	                   "update gmbsrelation set actived=1 where bsrelationname='"
	                   + bsrelationname + "'", H2Conn);
	           
	         
	           return bsrelationname + ":检查成功";
	       } catch (Exception e) {
	           e.printStackTrace();
	           throw new Exception(e.getMessage());
	       }
	}   

   /**
    * 查询语句生成， 前置查询，插入数据量语句。 后续查询语句，更新回填量语句，插入明细表语句。
    * 
    * @param type
    * @param bsrelationname
    * @param direction
    * @param formID
    * @param dataSetID
    * @param itemName
    * @param physicaltable
    * @param sqlList
    * @param bizConn
    * @throws SQLException
    * 
    */
   private void bsrelationByFormid(String type, String bsrelationname,
           String direction, String formID, String dataSetID, String itemName,
           String physicaltable, List<String> sqlList,Connection bizConn)
   throws Exception {

       String itemfield = "";
       PreparedStatement pst = null;
       
       // --检查有没有对应的角度没有设置
       
       pst = bizConn.prepareStatement("select a.columncaption,a.columnid,b.datasetid,b.frontmastername from gmdtstablecolumn a left join gmbsrelationbinddim  b  "
               + " on a.dtsid=b.bsrelationname and a.columncaption=b.mastername and b.formid="
               + formID
               + " and b.type='"
               + type
               + "' "
               + " where a.dtsid='"
               + bsrelationname
               + "' and a.iskey=1 "
               + " and( b.mastername='' or (nullif(b.mastername,null)=null))");
       ResultSet checkds = pst.executeQuery();
       String error = "";
       while (checkds.next()) {
           error = error + checkds.getString("columncaption") + ",";
       }
       checkds.close();
       pst.close();
       if (!"".equals(error)) {
           throw new Exception("请设置角度:" + error + "");
       }

       // /---整理对应字段关系
       Map<String, List<Map<String, String>>> FieldMap = new HashMap<String, List<Map<String, String>>>();

       pst = bizConn.prepareStatement("select a.columncaption,a.columnid,b.datasetid,b.frontmastername from gmdtstablecolumn a left join gmbsrelationbinddim  b  "
               + " on a.dtsid=b.bsrelationname and a.columncaption=b.mastername and b.formid="
               + formID
               + " and b.type='"
               + type
               + "' "
               + " where a.dtsid='"
               + bsrelationname
               + "' and a.iskey=1 ");
       ResultSet fieldds = pst.executeQuery();
       
       while (fieldds.next()) {
           String columncaption = fieldds.getString("columncaption");
           String columnid = fieldds.getString("columnid");
           String datasetid = fieldds.getString("datasetid");
           String frontmastername = fieldds.getString("frontmastername");

           List<Map<String, String>> columnlist;
           if (FieldMap.get(datasetid) == null) {
               columnlist = new ArrayList<Map<String, String>>();
               FieldMap.put(datasetid, columnlist);
           } else {
               columnlist = FieldMap.get(datasetid);
           }
           Map<String, String> columnMap = new HashMap<String, String>();
           columnMap.put("columnid", columnid);
           columnMap.put("columncaption", columncaption);
           columnMap.put("formcolumncaption", frontmastername);
           columnlist.add(columnMap);
       }
       fieldds.close();
       pst.close();

       // ---补充业务查找单号
       Map<String, String> docMap = new HashMap<String, String>();
       pst = bizConn.prepareStatement("select datasetid,frontmastername "
               + "from gmbsrelationbinddim  where bsrelationname='"
               + bsrelationname + "' and formid='" + formID
               + "' and type='" + type + "' and mastername='关键业务单号'");
       ResultSet findbackDoc = pst.executeQuery();
       while (findbackDoc.next()) {
           docMap.put(findbackDoc.getString("datasetid"), findbackDoc
                   .getString("frontmastername"));
       }
       findbackDoc.close();
       pst.close();

       // ---整理关系
       String tempdatasetid = dataSetID;
       List<Map<String, String>> datasetLink = new ArrayList<Map<String, String>>();
       while (true) {
           if ("".equals(tempdatasetid)) {
               break;
           }
           pst = bizConn.prepareStatement("select datasetid,fdatasetid,fdatasetname,masterdatasetid,masterfields,detailfields from gmmodeldataset  "
                   + " where modelid in (select modelid from gsformmodellay where formid="
                   + formID
                   + ") "
                   + " and datasetid='"
                   + tempdatasetid + "'");
           ResultSet gmmodeldataset = pst.executeQuery();
           Map<String, String> linkMap = new HashMap<String, String>();
           boolean find = false;
           while (gmmodeldataset.next()) {
               tempdatasetid = PublicFunc.reStringByNull(gmmodeldataset
                       .getString("masterdatasetid"));

               linkMap.put("datasetid", PublicFunc
                       .reStringByNull(gmmodeldataset.getString("datasetid")));
               linkMap
               .put("fdatasetid", PublicFunc
                       .reStringByNull(gmmodeldataset
                               .getString("fdatasetid")));
               linkMap.put("fdatasetname", PublicFunc
                       .reStringByNull(gmmodeldataset
                               .getString("fdatasetname")));
               linkMap.put("masterdatasetid", tempdatasetid);
               linkMap.put("masterfields", PublicFunc
                       .reStringByNull(gmmodeldataset
                               .getString("masterfields")));
               linkMap.put("detailfields", PublicFunc
                       .reStringByNull(gmmodeldataset
                               .getString("detailfields")));

               datasetLink.add(linkMap);
               find = true;
           }
           gmmodeldataset.close();
           pst.close();
           
           if (!find) {
               break;
           }
       }

       // ---构建语句
       String SqlStr = "";
       for (int i = datasetLink.size() - 1; i >= 0; i--) {

           Map<String, String> linkMap = datasetLink.get(i);
           String datasetid = linkMap.get("datasetid");
           String masterdatasetid = linkMap.get("masterdatasetid");
           String fdatasetid = linkMap.get("fdatasetid");
           String fdatasetname = linkMap.get("fdatasetname");
           String masterfields = linkMap.get("masterfields");
           String detailfields = linkMap.get("detailfields");
           String fieldcaption = "", fieldname = "", fieldvalue = "";

           if ("".equals(SqlStr)) {
               SqlStr = " T" + datasetid;
           } else {
               String modelStr = "";
               SqlStr = SqlStr + " left join " + " T" + datasetid + " on ";

               String[] masterfieldlist = masterfields.split(";");
               String[] detailfieldlist = detailfields.split(";");
               for (int d = 0; d < masterfieldlist.length; d++) {
                   if (!"".equals(modelStr)) {
                       modelStr = modelStr + " and ";
                   }
                   modelStr = modelStr + "  T" + masterdatasetid + "."
                   + masterfieldlist[d] + "=T" + datasetid + "."
                   + detailfieldlist[d];
               }
               SqlStr = SqlStr + modelStr;

           }

           if (fdatasetid.equalsIgnoreCase("")) {
               fieldcaption = "columncaption";
               fieldname = "columnid";

           } else {
               fieldcaption = "fieldcaption";
               fieldname = "fieldname";

           }
           // ---整理字段对应的
           System.out.println(FieldMap);
           List<Map<String, String>> columnlist = FieldMap.get(datasetid);

           if (columnlist != null) {
               ResultSet fieldDs = findFieldByFdatasetid(fdatasetid,
                       fdatasetname,bizConn);

               while (fieldDs.next()) {
                   fieldvalue = fieldDs.getString(fieldcaption);
                   for (int x = 0; x < columnlist.size(); x++) {
                       if (fieldvalue.equals(columnlist.get(x).get(
                               "formcolumncaption"))) {
                           columnlist.get(x).put("formcolumnid",
                                   fieldDs.getString(fieldname));
                           break;
                       }
                   }

                   if (datasetid.equals(dataSetID)
                           && fieldvalue.equals(itemName)) {
                       itemfield = fieldDs.getString(fieldname);
                   }
               }
           }

           if (docMap.get(datasetid) != null) {
               ResultSet fieldDs = findFieldByFdatasetid(fdatasetid,
                       fdatasetname,bizConn);
               String docfield = docMap.get(datasetid);
               while (fieldDs.next()) {
                   fieldvalue = fieldDs.getString(fieldcaption);
                   for (int x = 0; x < columnlist.size(); x++) {
                       if (fieldvalue.equals(docfield)) {
                           docMap = new HashMap<String, String>();
                           docMap.put("关键业务单号", "T" + datasetid + "."
                                   + fieldDs.getString(fieldname));
                           break;
                       }
                   }
               }
           }
           // --

       }

       if ("".equals(itemfield)) {
           throw new Exception("业务关联：" + bsrelationname + "," + type
                   + "绑定数据项：" + itemName + ",在数据集编号为:" + dataSetID + ",找不到");
       }

       String errorStr = "";
       // ---整理前置，后续语句 physicaltable
       String selectStr = "", insertStr = "", insertvalue = "", updateStr = "", refdoccodeStr = "";
       Set<String> key = FieldMap.keySet();
       for (Iterator it = key.iterator(); it.hasNext();) {
           String tempdataset = (String) it.next();
           List<Map<String, String>> columnlist = FieldMap.get(tempdataset);

           for (int i = 0; i < columnlist.size(); i++) {
               Map<String, String> columnmap = columnlist.get(i);

               String formcolumnid = columnmap.get("formcolumnid");
               String columnid = columnmap.get("columnid");
               String columncaption = columnmap.get("columncaption");

               if (formcolumnid == null || "".equals(formcolumnid)) {
                   errorStr = errorStr + columncaption + ",";
               }
               // --查询
               if (!"".equals(selectStr)) {
                   selectStr = selectStr + ",";
                   updateStr = updateStr + " and ";
                   insertStr = insertStr + ",";
                   insertvalue = insertvalue + ",";
               }

               selectStr = selectStr + "T" + tempdataset + "." + formcolumnid;
               updateStr = updateStr + columnid + "=? ";
               insertStr = insertStr + columnid;
               insertvalue = insertvalue + "?";

               if ("单号".equals(columncaption)) {
                   String findDocStr = PublicFunc.reStringByNull(docMap
                           .get("关键业务单号"));
                   if (findDocStr != null) {
                       findDocStr = "," + findDocStr;
                   } else {
                       findDocStr = "";
                   }
                   refdoccodeStr = "select distinct T" + tempdataset + "."
                   + formcolumnid + "  from " + SqlStr;
               }
           }
       }
       if (!"".equals(errorStr)) {
           throw new Exception(type + "," + formID + ",关系角度：" + errorStr
                   + "没有找到了字段");
       }

       if ("前置".equals(type)) {
           // --查询
           String temp = "select " + selectStr + ",sum(T" + dataSetID + "."
           + itemfield + ") as " + itemfield + " from " + SqlStr
           + " group by " + selectStr;
           sqlList.add("update gmbsrelationbind set selectstr='"
                   + temp.replaceAll("'", "''") + "' where "
                   + "bsrelationname='" + bsrelationname + "' and formid='"
                   + formID + "' and type='" + type + "'");
           // --插入数据量
           temp = "insert into " + physicaltable + "(" + insertStr
           + ",field1,field2)values(" + insertvalue + ",?,0)";
           sqlList.add("update gmbsrelationbind set insertStr='"
                   + temp.replaceAll("'", "''") + "' where "
                   + "bsrelationname='" + bsrelationname + "' and formid='"
                   + formID + "' and type='" + type + "'");
       } else if ("后续".equals(type)) {
           // --查询
           String temp = "select sum(T" + dataSetID + "." + itemfield
           + ") as " + itemfield + "," + selectStr + " from " + SqlStr
           + " group by " + selectStr;
           sqlList.add("update gmbsrelationbind set selectstr='"
                   + temp.replaceAll("'", "''") + "' where "
                   + "bsrelationname='" + bsrelationname + "' and formid='"
                   + formID + "' and type='" + type + "'");
           temp = "update " + physicaltable + " set field2=field2" + direction
           + "? where " + updateStr;
           // -更新数据量
           sqlList.add("update gmbsrelationbind set updateStr='"
                   + temp.replaceAll("'", "''") + "',finddoccodeStr='"
                   + refdoccodeStr.replaceAll("'", "''") + "' where "
                   + "bsrelationname='" + bsrelationname + "' and formid='"
                   + formID + "' and type='" + type + "'");

       }

   }
   private ResultSet findFieldByFdatasetid(String fdatasetid,
           String fdatasetname ,Connection bizConn) throws Exception {

       ResultSet fieldRs = null;

       if (fdatasetid.equalsIgnoreCase("")) {
//           DtsManager Dts = new DtsManager(fdatasetname, "", bizConn, bizMetaConn,null,
//                   null);
//           fieldRs = Dts.getDataFieldRs();
       	// --列信息--暂时将单据信息放在这里，以后再移出去
   		String sqlfieldStr = "SELECT a.dtsid, a.columncaption, a.columnid, a.datatype, a.iskey, b.h2str, b.notspace,"
   				+ " c. NO, c.listshow, c.listlength, c.viewplugtype, a.mastername, a.propname, c.defaultvalue, c.dateformat "
   				+ "FROM gmDTStablecolumn a LEFT JOIN gmdatatype b ON a.datatype = b.datatype"
   				+ " LEFT JOIN gmdtsview c ON a.dtsid = c.dtsid AND a.columncaption = c.columncaption "
   				+ "WHERE a.dtsid = '" + fdatasetname + "' ORDER BY NO;";
   		fieldRs = PublicFunc.getResultSetBySql(sqlfieldStr, bizConn);

       } else {
//           DataSetManager ds = new DataSetManager(fdatasetid, "", bizConn,bizMetaConn,
//                   null, null);
//
//           fieldRs = ds.getDataFieldRs();
       	// --列信息
   		String sqlfieldStr = "select a.datasetid,a.fieldcaption,a.fieldname,a.datatype,a.profilefield,a.datarevolvefield,"
   				+ "	a.mastername,a.tablename,a.propname,a.iskey,b.h2str,b.notspace,a.no,a.fieldcaption as columncaption,a.fieldname as columnid ,a.tablename  "
   				+ "from gmdatastore a left join gmdatatype b on  a.datatype=b.datatype "
   				+ "where a.datasetid='" + fdatasetid + "'  order by no ";
   		fieldRs = PublicFunc.getResultSetBySql(sqlfieldStr, bizConn);
       }

       return fieldRs;
   }
   private void upDateBySql(String sql,Connection conn)throws Exception{
       log.info(sql);
       PreparedStatement preparedStatement = conn.prepareStatement(sql);
       preparedStatement.execute();
       preparedStatement.close();
   } 
}
