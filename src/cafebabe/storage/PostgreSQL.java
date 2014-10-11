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
import cafebabe.qqwry.Utils;

public class PostgreSQL {
	private String username = "postgres";
	private String password = "postgres";
	protected String driverClassName = "org.postgresql.Driver";
	private String jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/ipsdb";
	private static PostgreSQL instance = null;

	public synchronized static PostgreSQL getInstance() {
		if (null == instance)
			instance = new PostgreSQL();
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
			pst = conn
					.prepareStatement("select tablename from pg_tables where tablename = ? and schemaname='public'");
			pst.setString(1, tableName);
			rs = pst.executeQuery();
			return rs.next();
		} catch (SQLException ex) {
			ex.printStackTrace();
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
		String sql = "create table "
				+ tableName
				+ " (id serial not null, ip_start inet, ip_end inet, country varchar(255), area varchar(255), "
				+ "constraint \"" + tableName + "_pkey\" primary key (id))";
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
		PostgreSQL pg = PostgreSQL.getInstance();
		Connection conn = null;
		PreparedStatement pst = null;
		String tableName = "ips";
		String sql = "insert into "
				+ tableName
				+ " (ip_start, ip_end, country, area) values(cast(? as inet), cast(? as inet), ?, ?)";
		try {
			conn = pg.getConnection();
			if (!pg.isExistTalbe(conn, tableName))
				pg.createTable(conn, tableName);
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);

			int count = 0;
			long pos = header.getIpBegin();
			while (pos <= header.getIpEnd()) {
				index = new QQWryIndex(ipFile, pos);
				record = new QQWryRecord(ipFile, index.getStartIp(),
						index.getIpPos());

				pst.setString(1, Utils.ipToStr(record.getBeginIP()));
				pst.setString(2, Utils.ipToStr(record.getEndIP()));
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
