package com.victop.tool.dataarchitecture.active.bsrelation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.tool.dataarchitecture.Active;
import com.victop.tool.dataarchitecture.active.DtsActive;

/**
 * 业务关联原表生效"
 * @author chenzr
 *
 */
public class BsRelationDtsActived extends Active {
	
    private Logger log = LoggerFactory.getLogger(BsRelationDtsActived.class);
    
	@Override
	public String execute(Map<String, String> params, String resourceid,
			String resourceGuid, String schemaverGuid,
			Map<String, Connection> connMap) throws Exception {
		  // --业务查询
        String bsrelationname = resourceid;

        try {
        	String sql = "SELECT businesstype FROM gmBsRelation" +
        			"  WHERE bsrelationname = '"+resourceid+"'";
            String businesstype = "";
            ResultSet rSet = getResultSetBySql(H2Conn, sql);
            if(rSet.next()){
            	businesstype = rSet.getString("businesstype");
            }
            closeResultSet(rSet);
            InsertBsRelationDts(bsrelationname, businesstype, H2Conn);
            DtsActive active = new DtsActive();
            active.setConn(H2Conn, BizConn, ConfConn, memConn);
            active.execute(params, resourceid, resourceGuid, schemaverGuid, connMap);
            active.execute(params, resourceid, resourceGuid+ "_关系", schemaverGuid, connMap);

            return bsrelationname + ":生效成功";

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
	}


    private String InsertBsRelationDts(String bsrelationname,
            String businesstype, Connection bizConn) throws SQLException {

        String dtsTableName = "TR" + System.currentTimeMillis();

        try {
            upDateBySql(
                    "insert into gmmdldts(dtsid,dtstype,PhysicalTable,businesstype,sharecode)"
                    + "values('" + bsrelationname + "','业务关联','"
                    + dtsTableName + "','" + businesstype + "','')",
                    bizConn);
            upDateBySql(
                    "insert into gmmdldts(dtsid,dtstype,PhysicalTable,businesstype,sharecode)"
                    + "values('" + bsrelationname + "_关系','外部数据','"
                    + dtsTableName + "detail','" + businesstype
                    + "','')", bizConn);
        } catch (Exception e) {
        }

        String sqlWhere = "";

        upDateBySql("delete from gmdtsdim where dtsid in('"
                + bsrelationname + "' )", bizConn);
        upDateBySql("delete from gmdtscol where dtsid in('"
                + bsrelationname + "')", bizConn);

        // ----业务关联数据量
        upDateBySql(
                "insert into gmdtsdim(dtsid,dimno,dimname,dimprop,dimfield)"
                + "select '"
                + bsrelationname
                + "',1,a.mastername,a.propname,b.columnid "
                + "from gmbsrelationdim a left outer join gmmastertablecolumn b on  a.mastername=b.mastername and a.propname=b.columncaption "
                + "left join gmmaster c  on b.mastername=c.mastername "
                + "where a.bsrelationname='" + bsrelationname
                + "'" + sqlWhere, bizConn);

        PreparedStatement pst = bizConn.prepareStatement("select itemname,datatype from gmBsRelation a  where  bsrelationname='"
                + bsrelationname + "' " + sqlWhere);
        ResultSet col = pst.executeQuery();

        int x = 1;
        String feild = "", columntype = "";
        while (col.next()) {
            feild = "field" + Integer.toString(x);
            columntype = col.getString("datatype");
            upDateBySql(
                    "insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
                    + "values ('" + bsrelationname + "','"
                    + col.getString("itemname") + "','" + feild + "','"
                    + columntype + "')", bizConn);
            x = x + 1;

        }
        feild = "field" + Integer.toString(x);
        upDateBySql(
                "insert into gmdtscol(dtsid,columnid,columnfield,columntype)"
                + "values ('" + bsrelationname + "','回填量','" + feild
                + "','" + columntype + "')", bizConn);

        // ---"+bsrelationname + "_关系
        upDateBySql("delete from gmdtstablecolumn where dtsid in('"
                + bsrelationname + "_关系' )", bizConn);

        upDateBySql(
                "insert into gmdtstablecolumn(dtsid,columncaption,columnid,datatype,iskey)"
                + "values('" + bsrelationname
                + "_关系','前导业务系统编号 ','systemid','中字符',1)", bizConn);
        upDateBySql(
                "insert into gmdtstablecolumn(dtsid,columncaption,columnid,datatype,iskey)"
                + "values('" + bsrelationname
                + "_关系','前导业务功能号 ','formid','中字符',1)", bizConn);
        upDateBySql(
                "insert into gmdtstablecolumn(dtsid,columncaption,columnid,datatype,iskey)"
                + "values('" + bsrelationname
                + "_关系','前导业务单号 ','doccode','中字符',1)", bizConn);
        upDateBySql(
                "insert into gmdtstablecolumn(dtsid,columncaption,columnid,datatype,iskey)"
                + "values('" + bsrelationname
                + "_关系','后续业务系统编号 ','bindsystemid','中字符',1)", bizConn);
        upDateBySql(
                "insert into gmdtstablecolumn(dtsid,columncaption,columnid,datatype,iskey)"
                + "values('" + bsrelationname
                + "_关系','后续业务功能号 ','bindformid','中字符',1)", bizConn);
        upDateBySql(
                "insert into gmdtstablecolumn(dtsid,columncaption,columnid,datatype,iskey)"
                + "values('" + bsrelationname
                + "_关系','后续业务单号 ','binddoccode','中字符',1)", bizConn);

        return "";
    }
    private void upDateBySql(String sql,Connection conn)throws SQLException{
        log.info(sql);
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.execute();
        preparedStatement.close();
    }

}
