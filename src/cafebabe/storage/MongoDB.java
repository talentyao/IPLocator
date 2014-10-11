package cafebabe.storage;

import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import cafebabe.qqwry.QQWryFile;
import cafebabe.qqwry.QQWryHeader;
import cafebabe.qqwry.QQWryIndex;
import cafebabe.qqwry.QQWryRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;

public class MongoDB {
	private Mongo conn = null;
	private DB db = null;
	private DBCollection coll = null;

	private static MongoDB instance = null;

	public Mongo getConn() {
		return conn;
	}

	public DBCollection getColl() {
		return coll;
	}

	public MongoDB() {
		try {
			conn = new Mongo();
			db = conn.getDB("IPLocator");
			coll = db.getCollection("ips");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public MongoDB(String host, int port, String dbName, String collName) {
		try {
			conn = new Mongo(host, port);
			db = conn.getDB(dbName);
			coll = db.getCollection(collName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public synchronized static MongoDB getInstance(String host, int port,
			String dbName, String collName) {
		if (null == instance)
			instance = new MongoDB(host, port, dbName, collName);
		return instance;
	}

	public synchronized static MongoDB getInstance() {
		if (null == instance)
			instance = new MongoDB();
		return instance;
	}

	public static WriteResult insertToMongoDBByBatch(MongoDB mongo,
			List<DBObject> batchPush) {
		return mongo.getColl().insert(batchPush);
	}

	public void close(MongoDB mongo) {
		mongo.getConn().close();
		mongo = null;
	}

	public void storage(RandomAccessFile ipFile, int batch) {
		MongoDB mongo = MongoDB.getInstance();
		List<DBObject> batchPush = new ArrayList<DBObject>(batch);
		QQWryHeader header = new QQWryHeader(ipFile);
		QQWryIndex index = null;
		QQWryRecord record = null;
		DBObject doc = null;
		int count = 0;
		long pos = header.getIpBegin();
		while (pos <= header.getIpEnd()) {
			index = new QQWryIndex(ipFile, pos);
			record = new QQWryRecord(ipFile, index.getStartIp(),
					index.getIpPos());

			doc = new BasicDBObject();
			doc.put("ip_start", record.getBeginIP());
			doc.put("ip_end", record.getEndIP());
			doc.put("loc", record.getCountry());
			doc.put("isp", record.getArea());
			batchPush.add(doc);
			doc = null;

			if (count < batch)
				count++;
			else {
				MongoDB.insertToMongoDBByBatch(mongo, batchPush);
				batchPush.clear();
				count = 0;
			}

			pos += QQWryFile.IP_RECORD_LENGTH;
		}

		batchPush = null;
		record = null;
		index = null;
		header = null;
		mongo.close(mongo);
	}
}