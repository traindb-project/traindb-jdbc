package traindb.jdbc.core;

import java.io.IOException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

import org.checkerframework.checker.nullness.qual.Nullable;

import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBProperty;
import traindb.jdbc.util.TrainDBState;

public abstract class ConnectionFactory {
	public static QueryExecutor openConnection(String url, Properties info)  throws SQLException {
		String protoName = TrainDBProperty.PROTOCOL_VERSION.get(info);
		
		ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
		QueryExecutor queryExecutor = connectionFactory.openConnectionImpl(url, info);
		
		if (queryExecutor != null) {
			return queryExecutor;
		}

		throw new TrainDBJdbcException(MessageFormat.format("A connection could not be made using the requested protocol {0}.", protoName), TrainDBState.CONNECTION_UNABLE_TO_CONNECT);
	}

	public abstract QueryExecutor openConnectionImpl(String url, Properties info) throws SQLException;

	/**
	 * Safely close the given stream.
	 *
	 * @param newStream The stream to close.
	 */
	protected void closeStream(@Nullable TrainDBStream newStream) {
		if (newStream != null) {
			try {
				newStream.close();
			} catch (IOException e) {
			}
		}
	}
}
