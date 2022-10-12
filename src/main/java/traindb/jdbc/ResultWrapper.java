package traindb.jdbc;

import java.sql.ResultSet;

public class ResultWrapper {
	private final ResultSet rs;
	private final long updateCount;
	private final long insertOID;
	private ResultWrapper next;
	
	public ResultWrapper(ResultSet rs) {
		this.rs = rs;
		this.updateCount = -1;
		this.insertOID = -1;
	}

	public ResultWrapper(long updateCount, long insertOID) {
		this.rs = null;
		this.updateCount = updateCount;
		this.insertOID = insertOID;
	}

	public ResultSet getResultSet() {
		return rs;
	}

	public long getUpdateCount() {
		return updateCount;
	}

	public long getInsertOID() {
		return insertOID;
	}

	public ResultWrapper getNext() {
		return next;
	}

	public void append(ResultWrapper newResult) {
		ResultWrapper tail = this;
		
		while (tail.next != null) {
			tail = tail.next;
		}

		tail.next = newResult;
	}
}
