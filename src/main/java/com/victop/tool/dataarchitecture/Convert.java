package com.victop.tool.dataarchitecture;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 转换抽象类
 * @author chenzr
 *
 */
public abstract class Convert implements Builder {

	protected Logger logger = LoggerFactory.getLogger(Convert.class);
	
	//H2文件数据库连接
	protected Connection H2Conn = null;
	//数据架构数据库连接
	protected Connection BizConn = null;
	//配置信息数据库连接
	protected Connection ConfConn = null;
	//内存数据库连接
	protected Connection memConn = null;
	
	/**
	 * 转换接口
	 * @param params 参数
	 * @param resourceid 资源ID
	 * @param resourceGuid 资源GUID
	 * @param schemaverGuid 数据架构版本GUID
	 * @param connMap 自定义数据库连接
	 * @throws Exception
	 */
	public abstract String execute(Map<String, String> params, String resourceid ,String resourceGuid, String schemaverGuid,Map<String, Connection> connMap) throws Exception;
	
	/**
	 * 设置数据库连接
	 * @param _h2Conn H2文件数据库连接
	 * @param _bizConn 数据架构数据库连接
	 * @param _confConn 配置信息数据库连接
	 * @param _memConn 内存数据库连接
	 */
	public void setConn(Connection _h2Conn,Connection _bizConn,
			Connection _confConn,Connection _memConn) {
		setH2Conn(_h2Conn);
		setBizConn(_bizConn);
		setConfConn(_confConn);
		setMemConn(_memConn);
	}
	
	
	public Connection getH2Conn() {
		return this.H2Conn;
	}


	public void setH2Conn(Connection _h2Conn) {
		this.H2Conn = _h2Conn;
	}


	public Connection getBizConn() {
		return this.BizConn;
	}


	public void setBizConn(Connection _bizConn) {
		this.BizConn = _bizConn;
	}


	public Connection getConfConn() {
		return this.ConfConn;
	}


	public void setConfConn(Connection _confConn) {
		this.ConfConn = _confConn;
	}


	public Connection getMemConn() {
		return this.memConn;
	}


	public void setMemConn(Connection _memConn) {
		this.memConn = _memConn;
	}
	
	/**
	 * 更新数据
	 * 
	 * @param conn
	 * @param sql
	 * @throws SQLException
	 */
	public void upDateBySql(Connection conn, String sql) throws SQLException {
		Statement stmt = null;
		try {
			logger.info(sql);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		} finally {
			if (null != stmt) {
				if (!stmt.isClosed()) {
					stmt.close();
					stmt = null;
				}
			}
		}
	}


	/**
	 * 查询数据
	 * 
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getResultSetBySql(Connection conn, String sql)
			throws SQLException {
		logger.info(sql);
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}

	/**
	 * 关闭ResultSet
	 * 
	 * @param rs
	 */
	public void closeResultSet(ResultSet rs) {
		try {
			if (null != rs) {
				if (!rs.isClosed()) {
					Statement statement = rs.getStatement();
					rs.close();
					rs = null;
					statement.close();
					statement = null;
				}
			}
		} catch (Exception e) {
		}
	}
	
	public void closeStatement(Statement stmt) {
		try {
			if (null != stmt) {
				if (!stmt.isClosed()) {
					stmt.close();
					stmt = null;
				}
			}
		} catch (Exception e) {
		}
	}
}
