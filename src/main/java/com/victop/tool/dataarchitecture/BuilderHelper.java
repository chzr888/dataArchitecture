package com.victop.tool.dataarchitecture;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JTextArea;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.platform.common.util.CheckH2DataBase;
import com.victop.platform.common.util.PropertyUtil;
import com.victop.tool.dataarchitecture.log.Log;

public class BuilderHelper {

	private Logger logger = LoggerFactory.getLogger(BuilderHelper.class);
	private Log log = new Log();

	private static final String CONVERT_METHON_PACKAGE = "com.victop.tool.dataarchitecture.convert";
	private static final String ACTIVE_METHON_PACKAGE = "com.victop.tool.dataarchitecture.active";

	public static final PropertyUtil props = new PropertyUtil(
			"/config.properties");// 配置文件
	public Connection bizConn = null;
	public Connection confConn = null;
	public Connection memConn = null;
	public Connection h2Conn = null;

	/**
	 * 初始化数据库结构
	 * 
	 * @throws Exception
	 */
	public void initH2DB() throws Exception {
		try {
			CheckH2DataBase checkH2DataBase = new CheckH2DataBase();
			checkH2DataBase.checkDataBase(h2Conn);
			ArrayList<String> tableList = checkH2DataBase.getTableList();
			for (int i = 0; i < tableList.size(); i++) {
				upDateBySql(h2Conn, "DELETE FROM " + tableList.get(i));
			}
			// ---删除gmdatatablecolumn ,mtype,memo
			upDateBySql(h2Conn, "ALTER TABLE gmdtstablecolumn DROP COLUMN mtype");
			upDateBySql(h2Conn, "ALTER TABLE gmdtstablecolumn DROP COLUMN memo");

		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
	}

	/**
	 * 初始化连接
	 * 
	 * @throws Exception
	 */
	public void init(String schemaverguid) throws Exception {
		try {
			getH2Connection(schemaverguid);
			getBizConnection();
			getConfConnection();
			getMemConnection(schemaverguid);
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
	}

	/**
	 * 搜索资源
	 * 
	 * @throws Exception
	 */
	public void searchResource(String schemaverguid) throws Exception {
		ResultSet rs = null;
		try {
			String resourceTable = getTabelName(schemaverguid);
			upDateBySql(memConn, "DROP TABLE IF EXISTS " + resourceTable);
			// 建内存表
			String sql = "CREATE MEMORY LOCAL TEMPORARY TABLE  "
					+ resourceTable
					+ " (resourceId VARCHAR(50),Guid VARCHAR(50), typeId int(11) , schemaverguid VARCHAR(50),"
					+ " activeClassNames VARCHAR(50),typeName VARCHAR(50), actived  int(11),memo VARCHAR(12000),"
					+ " PRIMARY KEY (typeId,Guid,schemaverguid) ) NOT PERSISTENT";
			upDateBySql(memConn, sql);
			sql = "SELECT typeid,resourceSearchSql,activeClassNames,typename FROM gmresourcetype where actived = '1'"
					+ "  ORDER BY sortby";
			rs = getResultSetBySql(confConn, sql);
			while (rs.next()) {
				String typeid = rs.getString("typeid");
				String sqlt = rs.getString("resourceSearchSql");
				String activeClassNames = rs.getString("activeClassNames");
				String typeName = rs.getString("typename");
				if (null != sqlt && !"".equals(sqlt)
						&& !"null".equalsIgnoreCase(sqlt)) {
					ResultSet rSet = null;
					try {
						sqlt = sqlt
								.replaceAll("&schemaverguid&", schemaverguid);
						sqlt = sqlt + " ";
						rSet = getResultSetBySql(bizConn, sqlt);
						insertDataToMemTable(resourceTable, typeid,
								schemaverguid, activeClassNames, typeName, rSet);
					} catch (Exception e) {
						throw e;
					} finally {
						closeResultSet(rSet);
					}
				}
			}
			closeResultSet(rs);
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		} finally {
			closeResultSet(rs);
		}
	}

	/**
	 * 插入资源到内存表
	 * 
	 * @param rSet
	 * @throws SQLException
	 */
	private void insertDataToMemTable(String resourceTable, String typeid,
			String schemaverguid, String activeClassNames, String typeName,
			ResultSet rs) throws SQLException {
		String sql = "INSERT INTO "
				+ resourceTable
				+ " (resourceid,Guid,typeId,schemaverguid,activeClassNames,typeName) VALUES (?,?,?,?,?,?)";
		PreparedStatement pst = memConn.prepareStatement(sql);
		while (rs.next()) {
			log.println(".", false);
			pst.setString(1, rs.getString(1));
			pst.setString(2, rs.getString(2));
			pst.setString(3, typeid);
			pst.setString(4, schemaverguid);
			pst.setString(5, activeClassNames);
			pst.setString(6, typeName);
			pst.execute();
		}
	}

	/**
	 * 批量转换
	 * 
	 * @param schemaverguid
	 *            数据架构版本GUID
	 * @throws Exception
	 */
	public void batchConvert(String schemaverguid) throws Exception {
		ResultSet rs = null;
		try {
			String resourceTable = getTabelName(schemaverguid);
			String sql = "select typeId,resourceId,guid from " + resourceTable
					+ " where schemaverguid = '" + schemaverguid + "' ";
			rs = getResultSetBySql(memConn, sql);
			while (rs.next()) {
				log.println(".", false);
				convert(rs.getString("typeId"), rs.getString("resourceid"),
						rs.getString("guid"), schemaverguid);
			}
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		} finally {
			closeResultSet(rs);
		}
	}

	/**
	 * 转换
	 * 
	 * @param type
	 *            类型
	 * @param resourceguid
	 *            资源GUID
	 * @param schemaverguid
	 *            数据架构版本GUID
	 * @param ConfConn
	 * @throws Exception
	 */
	public void convert(String type, String resourceid, String resourceguid,
			String schemaverguid) throws Exception {
		ResultSet rs = null;
		try {
			String sql = "SELECT * FROM gmresourcesync where type = '" + type
					+ "' AND actived = '1' ORDER BY num";
			rs = getResultSetBySql(confConn, sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			Map<String, String> params = new HashMap<String, String>();
			Map<String, Connection> connMap = new HashMap<String, Connection>();
			while (rs.next()) {
				params.clear();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					params.put(rsmd.getColumnLabel(i).toLowerCase(),
							rs.getString(rsmd.getColumnLabel(i)));
				}
				String classStr = rs.getString("classNames");
				if (null != classStr && !"".equals(classStr)
						&& !"null".equalsIgnoreCase(classStr)) {
					String[] classNames = classStr.split(",");
					for (int i = 0; i < classNames.length; i++) {
						String classPath = CONVERT_METHON_PACKAGE + "."
								+ classNames[i];
						Convert convert = (Convert) Class.forName(classPath)
								.newInstance();
						// SqlConvert convert = new SqlConvert();
						convert.setConn(h2Conn, bizConn, confConn, memConn);
						convert.execute(params, resourceid, resourceguid,
								schemaverguid, connMap);
					}
				}
			}
			closeResultSet(rs);
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		} finally {
			closeResultSet(rs);
		}
	}

	/**
	 * 批量生效
	 * 
	 * @param schemaverguid
	 * @return
	 * @throws Exception
	 */
	public String batchActive(String schemaverguid) throws Exception {
		ResultSet rs = null;
		try {
			String resourceTable = getTabelName(schemaverguid);
			String sql = "select typeId,resourceId,guid,activeClassNames from "
					+ resourceTable + " " + " where schemaverguid = '"
					+ schemaverguid + "'";
			rs = getResultSetBySql(memConn, sql);
			while (rs.next()) {
				String classStr = rs.getString("activeClassNames");
				String resourceguid = rs.getString("guid");
				try {
					h2Conn.setAutoCommit(false);
					bizConn.setAutoCommit(false);
					if (null != classStr && !"".equalsIgnoreCase(classStr)
							&& !"null".equalsIgnoreCase(classStr)) {
						String[] classNames = classStr.split(",");
						for (int i = 0; i < classNames.length; i++) {
							Active(rs.getString("typeId"),
									rs.getString("resourceid"),
									rs.getString("guid"), schemaverguid,
									classNames[i]);
						}
					}
					h2Conn.commit();
					bizConn.commit();
					PreparedStatement pst = memConn
							.prepareStatement("UPDATE " + resourceTable
									+ " SET actived = 1 WHERE guid = ?");
					pst.setString(1, resourceguid);
					pst.executeUpdate();
					pst.close();
				} catch (Exception e) {
					logger.error("{}", e);
					try {
						h2Conn.rollback();
						bizConn.rollback();
						PreparedStatement pst = memConn
								.prepareStatement("UPDATE "
										+ resourceTable
										+ " SET actived = 0 ,memo = ? WHERE guid = ?");
						pst.setString(1, e.getMessage());
						pst.setString(2, resourceguid);
						pst.executeUpdate();
						pst.close();
					} catch (Exception e2) {
						e2.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		} finally {
			closeResultSet(rs);
		}
		return null;
	}

	/**
	 * 生效
	 * 
	 * @param type
	 * @param resourceid
	 * @param resourceguid
	 * @param schemaverguid
	 * @return
	 * @throws Exception
	 */
	private String Active(String type, String resourceid, String resourceguid,
			String schemaverguid, String activeClassName) throws Exception {
		Map<String, Connection> connMap = new HashMap<String, Connection>();
		try {
			String classPath = ACTIVE_METHON_PACKAGE + "." + activeClassName;
			Active active = (Active) Class.forName(classPath).newInstance();
			active.setConn(h2Conn, bizConn, confConn, memConn);
			active.execute(null, resourceid, resourceguid, schemaverguid,
					connMap);
		} catch (Exception e) {
			throw e;
		}
		return null;
	}

	/**
	 * 生效失败
	 * 
	 * @return
	 */
	public boolean effectiveFailure(String schemaverguid) {
		ResultSet rs = null;
		boolean b = false;
		try {
			String resourceTable = getTabelName(schemaverguid);
			String sql = "select 1 from " + resourceTable + " "
					+ "where schemaverguid = '" + schemaverguid
					+ "' And actived = 0";
			rs = getResultSetBySql(memConn, sql);
			if (rs.next()) {
				b = true;
			}
			closeResultSet(rs);

			sql = "select typeId,typeName,resourceId,guid,memo,actived from "
					+ resourceTable + " " + "where schemaverguid = '"
					+ schemaverguid + "' And actived = 0";
			rs = getResultSetBySql(memConn, sql);
			if (b) {
				logger.info("<<==============【" + schemaverguid
						+ "】生效失败的资源START=============>");
				log.println("<<==============【" + schemaverguid
						+ "】生效失败的资源STAR==============>");
			}

			while (rs.next()) {
				String typeName = rs.getString("typeName");
				String resourceId = rs.getString("resourceId");
				String memo = rs.getString("memo");
				logger.error("资源类型：" + typeName + " ==> 资源ID：" + resourceId
						+ " 错误信息：" + memo + "");
				log.println("资源类型：{0} ==> 资源ID：{1} 错误信息：{2}", typeName,
						resourceId, memo);
			}
			if (b) {
				logger.info("<<==============【" + schemaverguid
						+ "】生效失败的资源END===============>");
				log.println("<<==============【" + schemaverguid
						+ "】生效失败的资源END===============>");
			}

			return b;

		} catch (Exception e) {
			logger.error("{}", e);
		} finally {
			closeResultSet(rs);
		}
		return false;
	}

	/**
	 * 返回临时表名
	 * 
	 * @param schemaverguid
	 * @return
	 */
	private String getTabelName(String schemaverguid) {
		String resourceTable = schemaverguid.replaceAll("-", "");
		return "T_" + resourceTable;
	}

	/**
	 * 返回H2数据库连接
	 * 
	 * @return
	 * @throws Exception
	 */
	private Connection getH2Connection(String schemaverguid) throws Exception {
		try {
			if (null != this.h2Conn) {
				return this.h2Conn;
			}
			Class.forName("org.h2.Driver");
			this.h2Conn = DriverManager.getConnection("jdbc:h2:./h2db/"
					+ schemaverguid, "", "");
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
		return this.h2Conn;
	}

	/**
	 * 返回数据架构数据库连接
	 * 
	 * @return
	 * @throws Exception
	 */
	private Connection getBizConnection() throws Exception {
		try {
			// String url =
			// "jdbc:sqlserver://192.168.25.134;DatabaseName=30vems;user=xmdev;password=xmdev";
			String url = props.get("bizdatabase.url");
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			bizConn = DriverManager.getConnection(url);
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
		return bizConn;
	}

	/**
	 * 返回配数据库连接
	 * 
	 * @return
	 * @throws Exception
	 */
	private Connection getConfConnection() throws Exception {
		try {
			if (null != confConn) {
				return confConn;
			}
			// String url =
			// "jdbc:mysql://127.0.0.1:3306/tt?user=root&password=root&zeroDateTimeBehavior=convertToNull";
			String url = props.get("confdatabase.url");
			// 加载驱动程序
			Class.forName("com.mysql.jdbc.Driver");
			// 连续数据库
			confConn = DriverManager.getConnection(url);
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
		return confConn;
	}

	/**
	 * 返回H2数据库连接
	 * 
	 * @return
	 * @throws Exception
	 */
	private Connection getMemConnection(String schemaverguid) throws Exception {
		try {
			if (null != memConn) {
				return memConn;
			}
			Class.forName("org.h2.Driver");
			memConn = DriverManager.getConnection("jdbc:h2:mem:"
					+ schemaverguid + "_db", "", "");
		} catch (Exception e) {
			logger.error("{}", e);
			throw e;
		}
		return memConn;
	}

	/**
	 * 提交事务
	 */
	public void commitConn(Connection conn) {
		try {
			if (null != conn) {
				if (!conn.isClosed()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
		}
	}

	/**
	 * 关闭连接
	 * 
	 * @param conn
	 */
	public void closeConn(Connection conn) {
		try {
			if (null != conn) {
				if (!conn.isClosed()) {
					conn.close();
					conn = null;
				}
			}
		} catch (Exception e) {
		}
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
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE);
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

	/**
	 * 关闭ResultSet
	 * 
	 * @param rs
	 */
	public void closeConn() {
		try {
			if (null != bizConn) {
				if (!bizConn.isClosed()) {
					bizConn.close();
					bizConn = null;
				}
			}
			if (null != confConn) {
				if (!confConn.isClosed()) {
					confConn.close();
					confConn = null;
				}
			}
			if (null != memConn) {
				if (!memConn.isClosed()) {
					memConn.close();
					memConn = null;
				}
			}
			if (null != h2Conn) {
				if (!h2Conn.isClosed()) {
					h2Conn.close();
					h2Conn = null;
				}
			}
		} catch (Exception e) {
		}
	}

	public void setLog(JTextArea _jTextArea1) {
		log.jTextArea1 = _jTextArea1;
	}

}
