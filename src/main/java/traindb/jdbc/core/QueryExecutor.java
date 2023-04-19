package traindb.jdbc.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import traindb.jdbc.TrainDBStatement.StatementResultHandler;
import traindb.jdbc.util.ServerErrorMessage;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public class QueryExecutor {
	private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());

	private TrainDBStream stream;
	private boolean closed = false;

	int QUERY_NO_RESULTS = 4;
	int QUERY_BOTH_ROWS_AND_STATUS = 64;

	private String currentQuery = null;
	private Field[] currentFields = null;

	public QueryExecutor(TrainDBStream stream, Properties info) {
		this.stream = stream;
	}

	public void abort() {
		try {
			stream.getSocket().close();
		} catch (IOException e) {
			// ignore
		}

		closed = true;
	}

	public void close() {
		if (closed) {
			return;
		}

		try {
			LOGGER.log(Level.FINEST, " FE=> Terminate");
			sendCloseMessage();
			stream.flush();
			stream.close();
		} catch (IOException ioe) {
			LOGGER.log(Level.FINEST, "Discarding IOException on close:", ioe);
		}

		closed = true;
	}

	public boolean isClosed() {
		return closed;
	}

	public void sendCloseMessage() throws IOException {
		// TODO Auto-generated method stub
	}

	public synchronized void execute(String sql, StatementResultHandler handler) {
		try {
			sendSimpleQuery(sql, handler);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void execute(String sql, ParameterList parameters, StatementResultHandler handler) throws SQLException {
		try {
			sendSimpleQuery(sql, parameters, handler);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendSimpleQuery(String sql, StatementResultHandler handler) throws IOException {
		LOGGER.log(Level.FINEST, " FE=> SimpleQuery(query=\"{0}\")", sql);
		// Encoding encoding = stream.getEncoding();

		byte[] data = sql.getBytes();
		stream.sendChar('E');
		stream.sendInteger4(4 + data.length);
		stream.send(data);
		stream.flush();
		currentQuery = sql;

		// pendingExecuteQueue.add(new ExecuteRequest(query, null, true));
		// pendingDescribePortalQueue.add(query);

		processResults(handler, 0, false);
	}

	private void sendSimpleQuery(String sql, ParameterList parameters, StatementResultHandler handler) throws IOException {
		LOGGER.log(Level.FINEST, " FE=> SimpleQuery(query=\"{0}\")", sql);
		// Encoding encoding = stream.getEncoding();

		String nativeSql = getNativeSql(sql, parameters); // query.toString(params);

		byte[] data = nativeSql.getBytes();
		stream.sendChar('E');
		stream.sendInteger4(4 + data.length);
		stream.send(data);
		stream.flush();
		currentQuery = nativeSql;

		// pendingExecuteQueue.add(new ExecuteRequest(query, null, true));
		// pendingDescribePortalQueue.add(query);

		processResults(handler, 0, false);
	}

	public String getNativeSql(String sql, ParameterList parameters) {
		String nativeSql = sql;

	    if (parameters.getParamCount() == 0) {
	    	return nativeSql;
	    }

	    List<Integer> bindPositions = getBindPositions(sql);

		int queryLength = nativeSql.length();
		String[] params = new String[parameters.getParamCount()];
		for (int i = 1; i <= parameters.getParamCount(); ++i) {
			String param = parameters == null ? "?" : parameters.toString(i, true);
			params[i - 1] = param;
			queryLength += param.length() - 1;
		}

		StringBuilder sbuf = new StringBuilder(queryLength);
		sbuf.append(nativeSql, 0, bindPositions.get(0));

		for (int i = 1; i <= params.length; ++i) {
			sbuf.append(params[i - 1]);
			int nextBind = i < params.length ? bindPositions.get(i) : nativeSql.length();
			sbuf.append(nativeSql, bindPositions.get(i - 1) + 1, nextBind);
		}

		return sbuf.toString();
	}

	private List<Integer> getBindPositions(String sql) {
		List<Integer> bindPositions = new ArrayList<Integer> ();
		int index = sql.indexOf("?");

		while(index != -1) {
			bindPositions.add(index);
			index = sql.indexOf("?", index + "?".length());
		}

		return bindPositions;
	}

	protected void processResults(StatementResultHandler handler, int flags, boolean adaptiveFetch) throws IOException {
	    boolean noResults = (flags & QUERY_NO_RESULTS) != 0;
	    boolean bothRowsAndStatus = (flags & QUERY_BOTH_ROWS_AND_STATUS) != 0;

	    List<Tuple> tuples = null;

	    int c;
	    boolean endQuery = false;

	    // At the end of a command execution we have the CommandComplete
	    // message to tell us we're done, but with a describeOnly command
	    // we have no real flag to let us know we're done. We've got to
	    // look for the next RowDescription or NoData message and return
	    // from there.
	    boolean doneAfterRowDescNoData = false;

	    while (!endQuery) {
	    	c = stream.receiveChar();

	    	switch (c) {
	    		/*
	    		case 'A': // Asynchronous Notify
	    			receiveAsyncNotify();
	    			break;

	    		case '1': // Parse Complete (response to Parse)
	    			stream.receiveInteger4(); // len, discarded

	    			SimpleQuery parsedQuery = pendingParseQueue.removeFirst();
	    			String parsedStatementName = parsedQuery.getStatementName();

	    			LOGGER.log(Level.FINEST, " <=BE ParseComplete [{0}]", parsedStatementName);

	    			break;

	    		case 't': { // ParameterDescription
	    			stream.receiveInteger4(); // len, discarded

	    			LOGGER.log(Level.FINEST, " <=BE ParameterDescription");

	    			DescribeRequest describeData = pendingDescribeStatementQueue.getFirst();
	    			SimpleQuery query = describeData.query;
	    			SimpleParameterList params = describeData.parameterList;
	    			boolean describeOnly = describeData.describeOnly;
	    			// This might differ from query.getStatementName if the query was re-prepared
	    			String origStatementName = describeData.statementName;

	    			int numParams = stream.receiveInteger2();
	    			
	    			for (int i = 1; i <= numParams; i++) {
	    				int typeOid = stream.receiveInteger4();
	    				params.setResolvedType(i, typeOid);
	    			}

					// Since we can issue multiple Parse and DescribeStatement
					// messages in a single network trip, we need to make
					// sure the describe results we requested are still
					// applicable to the latest parsed query.
					//
					if ((origStatementName == null && query.getStatementName() == null) || (origStatementName != null && origStatementName.equals(query.getStatementName())))
					{
						query.setPrepareTypes(params.getTypeOIDs());
					}

					if (describeOnly) {
						doneAfterRowDescNoData = true;
					} else {
						pendingDescribeStatementQueue.removeFirst();
					}
					
					break;
				}

	    		case '2': // Bind Complete (response to Bind)
	    			stream.receiveInteger4(); // len, discarded

	    			Portal boundPortal = pendingBindQueue.removeFirst();
	    			LOGGER.log(Level.FINEST, " <=BE BindComplete [{0}]", boundPortal);

	    			registerOpenPortal(boundPortal);
	    			break;

	    		case '3': // Close Complete (response to Close)
	    			stream.receiveInteger4(); // len, discarded
	    			LOGGER.log(Level.FINEST, " <=BE CloseComplete");
	    			break;
				*/

	    		case 'n': // No Data (response to Describe)
	    			/*
	    			stream.receiveInteger4(); // len, discarded
	    			LOGGER.log(Level.FINEST, " <=BE NoData");

	    			pendingDescribePortalQueue.removeFirst();

	    			if (doneAfterRowDescNoData) {
	    				DescribeRequest describeData = pendingDescribeStatementQueue.removeFirst();
	    				SimpleQuery currentQuery = describeData.query;

	    				Field[] fields = currentQuery.getFields();

	    				if (fields != null) { // There was a resultset.
	    					tuples = new ArrayList<Tuple>();
	    					handler.handleResultRows(currentQuery, fields, tuples, null);
	    					tuples = null;
	    				}
	    			}
	    			*/

	    			break;
	    			
	    		/*
	    		case 's': { // Portal Suspended (end of Execute)
	    			// nb: this appears *instead* of CommandStatus.
	    			// Must be a SELECT if we suspended, so don't worry about it.

	    			stream.receiveInteger4(); // len, discarded
	    			LOGGER.log(Level.FINEST, " <=BE PortalSuspended");

	    			ExecuteRequest executeData = pendingExecuteQueue.removeFirst();
	    			SimpleQuery currentQuery = executeData.query;
	    			Portal currentPortal = executeData.portal;
	    			
	    			if (currentPortal != null) {
	    				// Existence of portal defines if query was using fetching.
	    				adaptiveFetchCache.updateQueryFetchSize(adaptiveFetch, currentQuery, stream.getMaxRowSizeBytes());
	    			}
	          
	    			stream.clearMaxRowSizeBytes();

	    			Field[] fields = currentQuery.getFields();
	          
	    			if (fields != null && tuples == null) {
	    				// When no results expected, pretend an empty resultset was returned
	    				// Not sure if new ArrayList can be always replaced with emptyList
	    				tuples = noResults ? Collections.<Tuple>emptyList() : new ArrayList<Tuple>();
	    			}

	    			if (fields != null && tuples != null) {
	    				handler.handleResultRows(currentQuery, fields, tuples, currentPortal);
	    			}
	    			
	    			tuples = null;

	    			break;
	    		}
	    		case 'C': { // Command Status (end of Execute)
	    			// Handle status.
	    			String status = receiveCommandStatus();
	    			if (isFlushCacheOnDeallocate() && (status.startsWith("DEALLOCATE ALL") || status.startsWith("DISCARD ALL"))) {
	    				deallocateEpoch++;
	    			}

	    			doneAfterRowDescNoData = false;

	    			ExecuteRequest executeData = castNonNull(pendingExecuteQueue.peekFirst());
	    			SimpleQuery currentQuery = executeData.query;
	    			Portal currentPortal = executeData.portal;

	    			if (currentPortal != null) {
	    				// Existence of portal defines if query was using fetching.

	    				// Command executed, adaptive fetch size can be removed for this query, max row size can be cleared
	    				adaptiveFetchCache.removeQuery(adaptiveFetch, currentQuery);
	    				// Update to change fetch size for other fetch portals of this query
	    				adaptiveFetchCache.updateQueryFetchSize(adaptiveFetch, currentQuery, stream.getMaxRowSizeBytes());
	    			}
	    			stream.clearMaxRowSizeBytes();

	    			if (status.startsWith("SET")) {
	    				String nativeSql = currentQuery.getNativeQuery().nativeSql;
	    				// Scan only the first 1024 characters to
	    				// avoid big overhead for long queries.
	    				if (nativeSql.lastIndexOf("search_path", 1024) != -1 && !nativeSql.equals(lastSetSearchPathQuery)) {
	    					// Search path was changed, invalidate prepared statement cache
	    					lastSetSearchPathQuery = nativeSql;
	    					deallocateEpoch++;
	    				}
	    			}

	    			if (!executeData.asSimple) {
	    				pendingExecuteQueue.removeFirst();
	    			} else {
	    				// For simple 'Q' queries, executeQueue is cleared via ReadyForQuery message
	    			}

	    			// we want to make sure we do not add any results from these queries to the result set
	    			if (currentQuery == autoSaveQuery || currentQuery == releaseAutoSave) {
	    				// ignore "SAVEPOINT" or RELEASE SAVEPOINT status from autosave query
	    				break;
	    			}

	    			Field[] fields = currentQuery.getFields();
	    			if (fields != null && tuples == null) {
	    				// When no results expected, pretend an empty resultset was returned
	    				// Not sure if new ArrayList can be always replaced with emptyList
	    				tuples = noResults ? Collections.<Tuple>emptyList() : new ArrayList<Tuple>();
	    			}

	    			// If we received tuples we must know the structure of the
	    			// resultset, otherwise we won't be able to fetch columns
	    			// from it, etc, later.
	    			if (fields == null && tuples != null) {
	    				throw new IllegalStateException("Received resultset tuples, but no field structure for them");
	    			}

	    			if (fields != null && tuples != null) {
	    				// There was a resultset.
	    				handler.handleResultRows(currentQuery, fields, tuples, null);
	    				tuples = null;

	    				if (bothRowsAndStatus) {
	    					interpretCommandStatus(status, handler);
	    				}
	    			} else {
	    				interpretCommandStatus(status, handler);
	    			}

	    			if (executeData.asSimple) {
	    				// Simple queries might return several resultsets, thus we clear
	    				// fields, so queries like "select 1;update; select2" will properly
	    				// identify that "update" did not return any results
	    				currentQuery.setFields(null);
	    			}

	    			if (currentPortal != null) {
	    				currentPortal.close();
	    			}
	    			break;
	    		}
				*/
	    		case 'C': // Command Complete
					int len = stream.receiveInteger4();
					String status = stream.receiveString(len - 5);
					stream.receiveChar();
	    			if (tuples != null) {
	    				handler.handleResultRows(currentQuery, currentFields, tuples, null);
	    				tuples = null;
	    			}
					endQuery = true;
	    			break;

	    		case 'D': // Data Transfer (ongoing Execute response)
	    			Tuple tuple = null;
	    			try {
	    				tuple = stream.receiveTuple();
	    			} catch (OutOfMemoryError oome) {
	    				if (!noResults) {
	    					handler.handleError(new TrainDBJdbcException("Ran out of memory retrieving query results.", TrainDBState.OUT_OF_MEMORY, oome));
	    				}
	    			} catch (SQLException e) {
	    				handler.handleError(e);
	    			}

	    			if (!noResults) {
	    				if (tuples == null) {
	    					tuples = new ArrayList<Tuple>();
	    				}
	    				if (tuple != null) {
	    					tuples.add(tuple);
	    				}
	    			}

	    			if (LOGGER.isLoggable(Level.FINEST)) {
	    				int length;
	    				if (tuple == null) {
	    					length = -1;
	    				} else {
	    					length = tuple.length();
	    				}

	    				LOGGER.log(Level.FINEST, " <=BE DataRow(len={0})", length);
	    			}
	    			break;

	    		case 'E':
	    			// Error Response (response to pretty much everything; backend then skips until Sync)
	    			SQLException error = receiveErrorResponse();
	    			handler.handleError(error);
	    			// keep processing
	    			break;

	    		/*
	    		case 'I': { // Empty Query (end of Execute)
	    			stream.receiveInteger4();

	    			LOGGER.log(Level.FINEST, " <=BE EmptyQuery");

	    			ExecuteRequest executeData = pendingExecuteQueue.removeFirst();
	    			Portal currentPortal = executeData.portal;
	    			handler.handleCommandStatus("EMPTY", 0, 0);
	    			if (currentPortal != null) {
	    				currentPortal.close();
	    			}
	    			break;
	    		}

	    		case 'N': // Notice Response
	    			SQLWarning warning = receiveNoticeResponse();
	    			handler.handleWarning(warning);
	    			break;

	    		case 'S': // Parameter Status
	    			try {
	    				receiveParameterStatus();
	    			} catch (SQLException e) {
	    				handler.handleError(e);
	    				endQuery = true;
	    			}
	    			break;
    			*/

	    		case 'T': // Row Description (response to Describe)
	    			currentFields = receiveFields();

					/*
	    			tuples = new ArrayList<Tuple>();

	    			SimpleQuery query = castNonNull(pendingDescribePortalQueue.peekFirst());
	    			if (!pendingExecuteQueue.isEmpty() && !castNonNull(pendingExecuteQueue.peekFirst()).asSimple) {
	    				pendingDescribePortalQueue.removeFirst();
	    			}
	    			query.setFields(fields);

	    			if (doneAfterRowDescNoData) {
	    				DescribeRequest describeData = pendingDescribeStatementQueue.removeFirst();
	    				SimpleQuery currentQuery = describeData.query;
	    				currentQuery.setFields(fields);

	    				handler.handleResultRows(currentQuery, fields, tuples, null);
	    				tuples = null;
	    			}
					 */
	    			break;

				/*
	    		case 'Z': // Ready For Query (eventual response to Sync)
	    			receiveRFQ();
	    			if (!pendingExecuteQueue.isEmpty() && castNonNull(pendingExecuteQueue.peekFirst()).asSimple) {
	    				tuples = null;
	    				stream.clearResultBufferCount();

	    				ExecuteRequest executeRequest = pendingExecuteQueue.removeFirst();
	    				// Simple queries might return several resultsets, thus we clear
	    				// fields, so queries like "select 1;update; select2" will properly
	    				// identify that "update" did not return any results
	    				executeRequest.query.setFields(null);

	    				pendingDescribePortalQueue.removeFirst();
	    				if (!pendingExecuteQueue.isEmpty()) {
	    					if (getTransactionState() == TransactionState.IDLE) {
	    						handler.secureProgress();
	    					}
	    					// process subsequent results (e.g. for cases like batched execution of simple 'Q' queries)
	    					break;
	    				}
	    			}
	    			endQuery = true;

	    			// Reset the statement name of Parses that failed.
	    			while (!pendingParseQueue.isEmpty()) {
	    				SimpleQuery failedQuery = pendingParseQueue.removeFirst();
	    				failedQuery.unprepare();
	    			}

	    			pendingParseQueue.clear(); // No more ParseComplete messages expected.
	    			// Pending "describe" requests might be there in case of error
	    			// If that is the case, reset "described" status, so the statement is properly
	    			// described on next execution
	    			while (!pendingDescribeStatementQueue.isEmpty()) {
	    				DescribeRequest request = pendingDescribeStatementQueue.removeFirst();
	    				LOGGER.log(Level.FINEST, " FE marking setStatementDescribed(false) for query {0}", request.query);
	    				request.query.setStatementDescribed(false);
	    			}
	    			while (!pendingDescribePortalQueue.isEmpty()) {
	    				SimpleQuery describePortalQuery = pendingDescribePortalQueue.removeFirst();
	    				LOGGER.log(Level.FINEST, " FE marking setPortalDescribed(false) for query {0}", describePortalQuery);
	    				describePortalQuery.setPortalDescribed(false);
	    			}
	    			pendingBindQueue.clear(); // No more BindComplete messages expected.
	    			pendingExecuteQueue.clear(); // No more query executions expected.
	    			break;
	    		 */
	    		default:
	    			throw new IOException("Unexpected packet type: " + c);
	    	}
	    }
	}

	private Field[] receiveFields() throws IOException {
		stream.receiveInteger4(); // MESSAGE SIZE
		int len = stream.receiveInteger2();
		Field[] fields = new Field[len];

		if (LOGGER.isLoggable(Level.FINEST)) {
			LOGGER.log(Level.FINEST, " <=BE RowDescription({0})", len);
		}

		for (int i = 0; i < len; i++) {
			String columnLabel = stream.receiveCanonicalString();
			//int tableOid = stream.receiveInteger4();
			//short positionInTable = (short) stream.receiveInteger2();
			int type = stream.receiveInteger4();
			int typeSize = stream.receiveInteger4();
			int format = stream.receiveInteger2();
			fields[i] = new Field(columnLabel, type, typeSize, format);

			LOGGER.log(Level.FINEST, "        {0}", fields[i]);
		}

		return fields;
	}

	private SQLException receiveErrorResponse() throws IOException {
		// it's possible to get more than one error message for a query
		// see libpq comments wrt backend closing a connection
		// so, append messages to a string buffer and keep processing
		// check at the bottom to see if we need to throw an exception

		int elen = stream.receiveInteger4();
		assert elen > 4 : "Error response length must be greater than 4";

		EncodingPredictor.DecodeResult totalMessage = stream.receiveErrorString(elen - 4);
		ServerErrorMessage errorMsg = new ServerErrorMessage(totalMessage);

		if (LOGGER.isLoggable(Level.FINEST)) {
			LOGGER.log(Level.FINEST, " <=BE ErrorMessage({0})", errorMsg.toString());
		}

		TrainDBJdbcException error = new TrainDBJdbcException(errorMsg);
		return error;
	}
}
