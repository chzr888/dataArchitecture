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

import javax.sql.rowset.CachedRowSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.modelfunction.model.PublicFunc;
import com.victop.tool.dataarchitecture.Active;

/**
 * "数据原表生效"
 * @author chenzr
 *
 */
public class DtsActive extends Active {

    private Logger log = LoggerFactory.getLogger(DtsActive.class);
    
	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		
		String dtsid = resourceid;
        log.info(dtsid);
        try {
            // --检查主档
        	 //创造日志
            String logtabledtsid = createLog(dtsid, H2Conn);
            dtsact(dtsid, H2Conn);
            if(!"".equals(logtabledtsid)){
                dtsact(logtabledtsid, H2Conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }finally{

        }
        return dtsid + "生效成功";
	}
    

    private String createLog(String dtsid,Connection bizConn) throws Exception {
        String logtablename = "";
        String physicaltable = "";
        int islog = 0;
        PreparedStatement pst = bizConn.prepareStatement("select physicaltable,islog from gmmdldts where dtsid = '" + dtsid + "'");
        ResultSet gmmdldts = pst.executeQuery();
        while(gmmdldts.next()){
            islog = gmmdldts.getInt("islog");
            physicaltable = gmmdldts.getString("physicaltable");
        }
        gmmdldts.close();
        pst.close();
        if(islog == 1){
            log.info("插入日志表的设置");
            logtablename = dtsid + "日志";
            boolean exist = false;
            pst = bizConn.prepareStatement("select 1 from gmmdldts where dtsid = '" + logtablename + "'");
            gmmdldts = pst.executeQuery();
            while(gmmdldts.next()){
                exist = true;
            }
            //需要生成日志表并未存在日志表
            if(!exist){
                physicaltable = physicaltable + "_log";
                log.info(logtablename);
                upDateBySql("insert into gmmdldts (dtsid,dtstype,Physicaltable,businesstype) " +
                        " values ('" + logtablename + "','外部数据','" + physicaltable + "','日志')", bizConn);
                upDateBySql("insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) " +
                        " values ('" + logtablename +"','唯一号','id','字符','1')",bizConn);
                upDateBySql("insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) " +
                        " values ('" + logtablename +"','修改人','modifyname','字符','0')", bizConn);
                upDateBySql("insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) " +
                        " values ('" + logtablename +"','修改时间','modifytime','日期','0')", bizConn);
                upDateBySql("insert into gmdtstablecolumn (dtsid,columncaption,columnid,datatype,iskey) " +
                        " values ('" + logtablename +"','修改数据','modifydata','longtext','0')", bizConn);
            }else{
            	logtablename = "";
            }
        }
        return logtablename;
    }

    private void dtsact(String dtsid,Connection bizConn)throws Exception{
		//去空格
    	String uptrimStr = "UPDATE gmdtstablecolumn SET columnid = trim(columnid),columncaption = trim(columncaption)  WHERE dtsid= '"+dtsid+"' AND columncaption = columncaption";
    	upDateBySql(uptrimStr, bizConn);

    	String sqlString = "select * from gmmdldts where dtsid='" + dtsid + "'";
        Statement  stmt = bizConn.createStatement();
        
        ResultSet gmmdldts = stmt.executeQuery(sqlString);

        CachedRowSet crs = new com.sun.rowset.CachedRowSetImpl();
        crs.populate(gmmdldts);
        gmmdldts.close();
        stmt.close();

        gmmdldts = crs;
        
        String reMsgString = DtsActiveCheck(gmmdldts, bizConn);
        if (!"".equals(reMsgString)) {
            throw new Exception(reMsgString);
        }

        // 维护后台表数据-------------------------------------------------------------------------------------------
        reMsgString = DtsActiveDealData(gmmdldts, bizConn);
        if (!"".equals(reMsgString)) {
            throw new Exception(reMsgString);
        }
        gmmdldts.close();
        // ----更新字段生效
        upDateBySql(
                "update gmdtscol set actived=1  where dtsid='" + dtsid
                + "'", bizConn);
        // ----修改状态
//        upDateBySql(
//                "update gmmdldts set actived=1  where dtsid='" + dtsid
//                + "'", bizConn);
        
        PreparedStatement pst = bizConn.prepareStatement("UPDATE gmmdldts SET actived = ? , stamp = ? WHERE dtsid = ?");
        pst.setInt(1, 1);
        pst.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        pst.setString(3, dtsid);
        pst.execute();
        pst.close();
        
        //外观
        upDateBySql("delete from gmdtsview where dtsid='"
                + dtsid
                + "' and  columncaption not in (select columncaption from gmDTStablecolumn where dtsid='"
                + dtsid + "')", bizConn);
        upDateBySql("insert into  gmdtsview (dtsid,columncaption)"
                + "select dtsid,columncaption from "
                + "gmDTStablecolumn where dtsid='"
                + dtsid
                + "' and  columncaption not in (select columncaption from gmdtsview where dtsid='"
                + dtsid + "')", bizConn);
//        upDateBySql("update gmdtsview set listshow = 1 where dtsid = '" + dtsid + "'" , bizConn);
    }

    /**
     * 检查数据原表设置
     * 
     * @param rs
     * @return
     * @throws SQLException
     */
    private String DtsActiveCheck(ResultSet gmmdldts, Connection bizConn)
    throws SQLException {
        boolean iscur = false;

        String dtsid = "", physicaltable = "", actived = "", dtstype = "";
        gmmdldts.beforeFirst();
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
                || "外部数据".equalsIgnoreCase(dtstype) 
                || "".equalsIgnoreCase(dtstype)
                || null == dtstype 
                || "业务规则".equalsIgnoreCase(dtstype)
                || "null".equalsIgnoreCase(dtstype)) {
            return "";
        }

        iscur = false;
        PreparedStatement pst = bizConn.prepareStatement("select * from gmdtsdim where dtsid='"
                + dtsid + "' ");
        ResultSet gmdtsdim = pst.executeQuery();
        while (gmdtsdim.next()) {
            iscur = true;
            break;
        }
        gmdtsdim.close();
        pst.close();
        if (!iscur) {
            // return "该数据原表没有设置角度，请检查!";
        }

        return "";
    }

    // ---整理数据原表后台数据
    private String DtsActiveDealData(ResultSet gmmdldts, Connection bizConn)
    throws SQLException {
        PreparedStatement pst = null;
        //用来判断是否是业务规则并且没有actived字段
        boolean BusinessRulesIsExistActived = false; 
        String dtsid = "", periodtype = "", dtstype = "", physicaltable = "", selectStr = "", sqlStr = "", keyStr = "";
        gmmdldts.beforeFirst();
        while (gmmdldts.next()) {
            dtsid = gmmdldts.getString("dtsid");
            physicaltable = gmmdldts.getString("physicaltable");
            periodtype = PublicFunc.reStringByNull(gmmdldts
                    .getString("periodtype"));
            dtstype = gmmdldts.getString("dtstype");
            dtstype = PublicFunc.reStringByNull(dtstype);
        }
        List<String> sqlList = new ArrayList<String>();
        
        if ("自定义数据".equalsIgnoreCase(dtstype) 
                || "外部数据".equalsIgnoreCase(dtstype) 
                || "null".equalsIgnoreCase(dtstype) 
                || null == dtstype 
        		|| "".equalsIgnoreCase(dtstype) 
        		|| "业务规则".equalsIgnoreCase(dtstype)
                ) {
        	//判断是否要加actived字段
            if("业务规则".equalsIgnoreCase(dtstype)){
            	ResultSet rSet2 = getResultSetBySql(bizConn, "select 1 from gmdtstablecolumn where dtsid='"+dtsid+"' and LOWER(columnid) ='actived'");
            	if(!rSet2.next()){
                	upDateBySql(H2Conn, InsertSqlByDtsTableColumn(dtsid, "生效", "actived", "整型", "0", "0", "", ""));
            	}
            	closeResultSet(rSet2);
        	}
        	      	
            pst = bizConn.prepareStatement("select * from gmdtstablecolumn where dtsid='" + dtsid
                    + "'");
            ResultSet myDsResultSet = pst.executeQuery();

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
            pst.close();
            if ("".equals(keyStr)) {
                return " 数据原表没有关键字段，请设置";
            }
            if ("".equals(selectStr)) {
                return "数据原表没有设置字段，请设置";
            }

            // ---构建sql语句 sql92
            sqlList.add("delete from gmdtssql where dtsid='" + dtsid + "'");
            sqlStr = "select " + selectStr + " from  " + physicaltable + " ";
            sqlList.add("insert into gmdtssql(dtsid,databasetype,sqlstr,isnew)values('"
                    + dtsid + "','sql92','" + sqlStr + "',0)");

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String data = df.format(new Date());
            // --数据字典
            sqlList.add("delete from gmmdlentity where LOWER(EntityID)=LOWER('"
                    + physicaltable + "')");
            sqlList.add("insert into gmmdlentity(entityid,entitycaption,keyfields,stamp)values('"
                    + physicaltable
                    + "','"
                    + dtsid
                    + "','"
                    + keyStr
                    + "','"+data+"')");

            for (int i = 0; i < sqlList.size(); i++) {
                upDateBySql(sqlList.get(i), bizConn);
            }

            return "";
        }

        // ---系统设置
        upDateBySql("delete from gmdtsfollowprop where dtsid='"
                + dtsid + "' and dimname in(  select mastername "
                + "from  GMDTStypeMaster  " + "where  dtstype='" + dtstype
                + "')", bizConn);

        upDateBySql("delete from gmdtsdim where dtsid='" + dtsid
                + "' and dimname in(  select mastername "
                + "from  GMDTStypeMaster  " + "where  dtstype='" + dtstype
                + "')", bizConn);

        upDateBySql(
                "insert into gmdtsdim(dtsid,dimno,dimname,dimprop,dimfield)"
                + "select '"
                + dtsid
                + "','-1',b.mastername,b.masterprop,c.propfield  "
                + "from  GMDTStypeMaster a join gmmaster b on a.mastername=b.mastername "
                + " join gmprop c on b.masterprop=c.propname "
                + "where  a.dtstype='" + dtstype + "'",
                bizConn);

        upDateBySql(
                "insert into gmdtsfollowprop(Dtsid,dimname,propname,propfield)"
                + "select '"
                + dtsid
                + "',b.mastername,b.propname,b.propfield  "
                + "from  GMDTStypeMaster a join gmmasterprop b on a.mastername=b.mastername "
                + "where  a.dtstype='" + dtstype + "'",
                bizConn);

        // --删除之前设置
        sqlList.add("delete from gmdtstablecolumn where dtsid='" + dtsid + "'");
        
        //数据检查，dimfield为空时回填
        String sqlString = "SELECT a.dtsid,a.dimname,a.dimprop,a.DimField,b.propname,b.propfield FROM gmdtsdim a INNER JOIN gmprop b ON a.dimprop = b.PropName WHERE dtsid = '" + dtsid + "' AND DimField = ''";
        pst = bizConn.prepareStatement(sqlString);
        ResultSet checkUpGmdtsdim = pst.executeQuery();
        while (checkUpGmdtsdim.next()) {
        	String upDataStr = "UPDATE gmdtsdim SET DimField = '"+checkUpGmdtsdim.getString("propfield")+"' WHERE dtsid = '" + dtsid + "' AND dimname = '" + checkUpGmdtsdim.getString("dimname") + "' AND dimprop = '" + checkUpGmdtsdim.getString("dimprop") + "'";
        	upDateBySql(upDataStr, bizConn);
		}
        checkUpGmdtsdim.close();
        pst.close();
        
        // --角度
        pst = bizConn.prepareStatement("select b.masterprop,a.dimfield,c.datatype,b.mastername,b.masterprop as propname from gmdtsdim a "
                + "join gmmaster b on a.dimname=b.mastername "
                + "join gmprop c on b.masterprop=c.propname "
                + "where a.dtsid='" + dtsid + "'");
        ResultSet gmdtsdim = pst.executeQuery();
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
            
            //判断是否有生效字段，如果没有。dtstype又是业务规则加是actived字段
            if ("actived".equalsIgnoreCase(gmdtsdim.getString("dimfield"))) {
            	BusinessRulesIsExistActived = true;
			}
        }
        gmdtsdim.close();
        pst.close();
        
        // // --期间
        // if (!"".equals(periodtype)) {
        // sqlList.add(InsertSqlByDtsTableColumn(dtsid,
        // gmdtsdim.getString("masterprop"), periodtype, "字符", "-1",
        // "1", "", ""));
        // keyStr = PublicFunc.reConnectString(keyStr, "periodid", ",");
        // }

        // --跟随属性
        pst = bizConn.prepareStatement("select a.propname,a.propfield,b.datatype,d.mastername,d.columncaption as qpropname "
                + "from gmdtsfollowprop a  "
                + "join gmprop b on a.propname=b.propname "
                + "left join gmmastertablecolumn d on a.dimname=d.mastername and a.propname=d.columncaption  "
                + "where a.dtsid='" + dtsid + "'");
        ResultSet gmdtsfollowprop = pst.executeQuery();
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
            
            //判断是否有生效字段，如果没有。dtstype又是业务规则加是actived字段
            if ("actived".equalsIgnoreCase(gmdtsfollowprop.getString("propfield"))) {
            	BusinessRulesIsExistActived = true;
			}
            
        }
        gmdtsfollowprop.close();
        pst.close();
        
        // --跟随属性
        pst = bizConn.prepareStatement("select * from gmdtscol "
                + "where dtsid='" + dtsid + "'");
        ResultSet gmdtscol = pst.executeQuery();
        while (gmdtscol.next()) {
            sqlList.add(InsertSqlByDtsTableColumn(dtsid, gmdtscol
                    .getString("columnID"), gmdtscol.getString("ColumnField"),
                    gmdtscol.getString("ColumnType"), "0", "0", "", "",gmdtscol.getInt("mtype")));
            selectStr = PublicFunc.reConnectString(selectStr, gmdtscol
                    .getString("ColumnField"), ",");
            //判断是否有生效字段，如果没有。dtstype又是业务规则加是actived字段
            if ("actived".equalsIgnoreCase(gmdtscol.getString("ColumnField"))) {
            	BusinessRulesIsExistActived = true;
			}
        }
        gmdtscol.close();
        pst.close();
        
        //判断是否要加actived字段
        if("业务规则".equalsIgnoreCase(dtstype)){
        	if(!BusinessRulesIsExistActived){
        		   sqlList.add(InsertSqlByDtsTableColumn(dtsid, "生效", "actived", "整型", "0", "0", "", ""));
                   selectStr = PublicFunc.reConnectString(selectStr, "actived", ",");
        	}
        }
        
        // ---构建sql语句 sql92
        sqlList.add("delete from gmdtssql where dtsid='" + dtsid + "'");
        sqlStr = "select " + selectStr + " from  " + physicaltable + " ";
        sqlList.add("insert into gmdtssql(dtsid,databasetype,sqlstr,isnew)values('"
                + dtsid + "','sql92','" + sqlStr + "',0)");

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String data = df.format(new Date());
		
        // --数据字典
        sqlList.add("delete from gmmdlentity where LOWER(entityid)=LOWER('" + physicaltable
                + "')");
        sqlList.add("insert into gmmdlentity(entityid,entitycaption,keyfields,stamp)values('"
                + physicaltable + "','" + dtsid + "','" + keyStr + "','"+data+"')");

        for (int i = 0; i < sqlList.size(); i++) {
            upDateBySql(sqlList.get(i), bizConn);
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
            String iskey, String mastername, String propname,int mtype) {
        String sqlString = "insert into gmdtstablecolumn(DtsID,columncaption,columnid,datatype,no,iskey,mastername,propname,mtype) "
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
            + mastername + "','" + propname + "','"+mtype+"')";
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

    private void upDateBySql(String sql,Connection conn)throws SQLException{
        log.info(sql);
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.execute();
        preparedStatement.close();
    }


}
