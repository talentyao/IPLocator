package cafebabe.storage;

import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import cafebabe.qqwry.QQWryFile;
import cafebabe.qqwry.QQWryHeader;
import cafebabe.qqwry.QQWryIndex;
import cafebabe.qqwry.QQWryRecord;

public class MySQL {
	private String username = "root";
	private String password = "123456";
	protected String driverClassName = "com.mysql.jdbc.Driver";
	private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/ipsdb";
	private static MySQL instance = null;

	public synchronized static MySQL getInstance() {
		if (null == instance)
			instance = new MySQL();
		return instance;
	}

	public Connection getConnection() throws SQLException {
		Connection conn;
		if (username != null)
			conn = DriverManager.getConnection(jdbcUrl, username, password);
		else
			conn = DriverManager.getConnection(jdbcUrl);
		return conn;
	}

	public boolean isExistTalbe(Connection conn, String tableName) {
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			pst = conn.prepareStatement("SELECT COUNT(1) FROM " + tableName
					+ " where 1!=1");
			rs = pst.executeQuery();
			return rs.next();
		} catch (SQLException ex) {
			return false;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	public boolean createTable(Connection conn, String tableName) {
		PreparedStatement pst = null;
		String sql = "CREATE TABLE `" + tableName + "` ("
				+ " `id` int(11) NOT NULL AUTO_INCREMENT,"
				+ " `ip_start` int(10) unsigned DEFAULT NULL,"
				+ " `ip_end` int(10) unsigned DEFAULT NULL,"
				+ " `country` varchar(255) DEFAULT NULL,"
				+ " `area` varchar(255) DEFAULT NULL,"
				+ " KEY (`id`), KEY (`ip_start`), KEY (`ip_end`))";
		try {
			conn = getConnection();
			pst = conn.prepareStatement(sql);
			return pst.execute();
		} catch (SQLException ex) {
			ex.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null) {
					pst.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	public void storage(RandomAccessFile ipFile, int batch) {
		QQWryHeader header = new QQWryHeader(ipFile);
		QQWryIndex index = null;
		QQWryRecord record = null;
		Connection conn = null;
		PreparedStatement pst = null;
		String tableName = "ips";
		String sql = "insert into " + tableName
				+ " (ip_start, ip_end, country, area) values(?, ?, ?, ?)";
		try {
			conn = getConnection();
			if (!isExistTalbe(conn, tableName))
				createTable(conn, tableName);
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);

			int count = 0;
			long pos = header.getIpBegin();
			while (pos <= header.getIpEnd()) {
				index = new QQWryIndex(ipFile, pos);
				record = new QQWryRecord(ipFile, index.getStartIp(),
						index.getIpPos());

				pst.setLong(1, record.getBeginIP());
				pst.setLong(2, record.getEndIP());
				pst.setString(3, record.getCountry());
				pst.setString(4, record.getArea());
				pst.addBatch();
				if (count < batch)
					count++;
				else {
					pst.executeBatch();
					conn.commit();
					pst.clearBatch();
					pst.clearParameters();
					count = 0;
				}
				pos += QQWryFile.IP_RECORD_LENGTH;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.setAutoCommit(true);
				if (pst != null) {
					pst.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}
