/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.jdbc;

import static traindb.jdbc.util.Nullness.castNonNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.jdbc.core.Field;
import traindb.jdbc.core.ResultCursor;
import traindb.jdbc.core.Tuple;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public class TrainDBStatement implements Statement {
  private static final String[] NO_RETURNING_COLUMNS = new String[0];
  protected boolean replaceProcessingEnabled = true;
  protected @Nullable ResultWrapper result = null;
  /**
   * Maximum number of rows to return, 0 = unlimited.
   */
  protected int maxrows = 0;
  private TrainDBConnection connection;
  private volatile boolean isClosed = false;

  public TrainDBStatement(TrainDBConnection trainDBConnection, int resultSetType,
                          int resultSetConcurrency, int resultSetHoldability) {
    this.connection = trainDBConnection;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (!executeWithFlags(sql, 0)) {
      throw new TrainDBJdbcException("No results were returned by the query.",
          TrainDBState.NO_DATA);
    }

    return getSingleResultSet();
  }

  public boolean executeWithFlags(String sql, int flags) throws SQLException {
    // return executeCachedSql(sql, flags, NO_RETURNING_COLUMNS);

    checkClosed();

    StatementResultHandler handler = new StatementResultHandler();

    synchronized (this) {
      result = null;
    }

    try {
      startTimer();
      System.out.println("==> Execute Start");
      connection.getQueryExecutor().execute(sql, handler);
      System.out.println("==> Execute End");
      // connection.getQueryExecutor().execute(queryToExecute, handler, maxrows, fetchSize, flags, adaptiveFetch);
    } finally {
      killTimerTask();
    }

    synchronized (this) {
      checkClosed();

      ResultWrapper currentResult = handler.getResults();

      result = currentResult;
    }

    synchronized (this) {
      checkClosed();
      return (result != null && result.getResultSet() != null);
    }
  }

  public boolean executeWithFlags(int flags) throws SQLException {
    checkClosed();
    throw new TrainDBJdbcException("Can''t use executeWithFlags(int) on a Statement.",
        TrainDBState.WRONG_OBJECT_TYPE);
  }

  protected ResultSet getSingleResultSet() throws SQLException {
    synchronized (this) {
      checkClosed();
      ResultWrapper result = this.result;

      if (result.getNext() != null) {
        throw new TrainDBJdbcException("Multiple ResultSets were returned by the query.",
            TrainDBState.TOO_MANY_RESULTS);
      }

      return result.getResultSet();
    }
  }

  protected void checkClosed() throws SQLException {
    if (isClosed()) {
      throw new TrainDBJdbcException("This statement has been closed.",
          TrainDBState.OBJECT_NOT_IN_STATE);
    }
  }

  private void startTimer() {
    /*
     * there shouldn't be any previous timer active, but better safe than sorry.
     */
		/*
	    cleanupTimer();

	    STATE_UPDATER.set(this, StatementCancelState.IN_QUERY);

	    if (timeout == 0) {
	      return;
	    }

	    TimerTask cancelTask = new TimerTask() {
	      public void run() {
	        try {
	          if (!CANCEL_TIMER_UPDATER.compareAndSet(PgStatement.this, this, null)) {
	            // Nothing to do here, statement has already finished and cleared
	            // cancelTimerTask reference
	            return;
	          }
	          PgStatement.this.cancel();
	        } catch (SQLException e) {
	        }
	      }
	    };

	    CANCEL_TIMER_UPDATER.set(this, cancelTask);
	    connection.addTimerTask(cancelTask, timeout);
	    */
  }

  private void killTimerTask() {
		/*
	    boolean timerTaskIsClear = cleanupTimer();
	    // The order is important here: in case we need to wait for the cancel task, the state must be
	    // kept StatementCancelState.IN_QUERY, so cancelTask would be able to cancel the query.
	    // It is believed that this case is very rare, so "additional cancel and wait below" would not
	    // harm it.
	    if (timerTaskIsClear && STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.IDLE)) {
	      return;
	    }

	    // Being here means someone managed to call .cancel() and our connection did not receive
	    // "timeout error"
	    // We wait till state becomes "cancelled"
	    boolean interrupted = false;
	    synchronized (connection) {
	      // state check is performed under synchronized so it detects "cancelled" state faster
	      // In other words, it prevents unnecessary ".wait()" call
	      while (!STATE_UPDATER.compareAndSet(this, StatementCancelState.CANCELLED, StatementCancelState.IDLE)) {
	        try {
	          // Note: wait timeout here is irrelevant since synchronized(connection) would block until
	          // .cancel finishes
	          connection.wait(10);
	        } catch (InterruptedException e) { // NOSONAR
	          // Either re-interrupt this method or rethrow the "InterruptedException"
	          interrupted = true;
	        }
	      }
	    }
	    if (interrupted) {
	      Thread.currentThread().interrupt();
	    }
	    */
  }

  public ResultSet createResultSet(String originalQuery, Field[] fields, List<Tuple> tuples,
                                   @Nullable ResultCursor cursor) throws SQLException {
    TrainDBResultSet newResult =
        new TrainDBResultSet(originalQuery, this, fields, tuples, cursor, getMaxRows(),
            getMaxFieldSize(), getResultSetType(), getResultSetConcurrency(),
            getResultSetHoldability(), getAdaptiveFetch());
    newResult.setFetchSize(getFetchSize());
    newResult.setFetchDirection(getFetchDirection());
    return newResult;
  }

  public boolean getAdaptiveFetch() {
    // return adaptiveFetch;
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws SQLException {
    synchronized (this) {
      if (isClosed) {
        return;
      }

      isClosed = true;
    }

    cancel();

    // closeForNextExecution();

    //closeImpl();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getMaxRows() throws SQLException {
    checkClosed();
    return maxrows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw new TrainDBJdbcException(
          "Maximum number of rows must be a value greater than or equal to 0.",
          TrainDBState.INVALID_PARAMETER_VALUE);
    }

    maxrows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // TODO Auto-generated method stub
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    // TODO Auto-generated method stub
  }

  @Override
  public void cancel() throws SQLException {
		/*
		if (statementState == StatementCancelState.IDLE) {
		      return;
		    }
		    if (!STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY,
		        StatementCancelState.CANCELING)) {
		      // Not in query, there's nothing to cancel
		      return;
		    }
		    // Synchronize on connection to avoid spinning in killTimerTask
		    synchronized (connection) {
		      try {
		        connection.cancelQuery();
		      } finally {
		        STATE_UPDATER.set(this, StatementCancelState.CANCELLED);
		        connection.notifyAll(); // wake-up killTimerTask
		      }
		    }
		*/
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return executeWithFlags(sql, 0);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    synchronized (this) {
      checkClosed();

      if (result == null) {
        return null;
      }

      return result.getResultSet();
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO Auto-generated method stub
    synchronized (this) {
      checkClosed();

      // send request another result set to server
      
    StatementResultHandler handler = new StatementResultHandler();

    synchronized (this) {
      result = null;
    }

    try {
      startTimer();
      System.out.println("==> getMoreResult Start");
      connection.getQueryExecutor().getMoreResult(handler);
      System.out.println("==> getMoreResult End");
      // connection.getQueryExecutor().execute(queryToExecute, handler, maxrows, fetchSize, flags, adaptiveFetch);
    } finally {
      killTimerTask();
    }

    synchronized (this) {
      checkClosed();

      ResultWrapper currentResult = handler.getResults();

      result = currentResult;
    }

      return false;
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    // TODO Auto-generated method stub
  }

  @Override
  public void clearBatch() throws SQLException {
    // TODO Auto-generated method stub
  }

  @Override
  public int[] executeBatch() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * ResultHandler implementations for updates, queries, and either-or.
   */
  public class StatementResultHandler {
    private ResultWrapper results;
    private ResultWrapper lastResult;

    private @Nullable SQLException firstException;
    private @Nullable SQLException lastException;

    ResultWrapper getResults() {
      return results;
    }

    private void append(ResultWrapper newResult) {
      if (results == null) {
        lastResult = results = newResult;
      } else {
        castNonNull(lastResult).append(newResult);
      }
    }

    public void handleResultRows(String fromQuery, Field[] fields, List<Tuple> tuples,
                                 @Nullable ResultCursor cursor) {
      try {
        ResultSet rs = TrainDBStatement.this.createResultSet(fromQuery, fields, tuples, cursor);
        append(new ResultWrapper(rs));
      } catch (SQLException e) {
        handleError(e);
      }
    }

    public void handleCommandStatus(String status, long updateCount, long insertOID) {
      append(new ResultWrapper(updateCount, insertOID));
    }

    public void handleWarning(SQLWarning warning) {
      // TrainDBStatement.this.addWarning(warning);
    }

    public void handleError(SQLException error) {
      if (firstException == null) {
        firstException = lastException = error;
        return;
      }
      castNonNull(lastException).setNextException(error);
      this.lastException = error;
    }

    public void handleCompletion() throws SQLException {
      SQLException firstException = this.firstException;
      if (firstException != null) {
        throw firstException;
      }
    }
  }
}
