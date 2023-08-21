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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import traindb.jdbc.core.ParameterList;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public class TrainDBPreparedStatement extends TrainDBStatement implements PreparedStatement {
  protected final TrainDBConnection connection;
  protected final String sql;
  protected final ParameterList preparedParameters; // Parameter values for prepared statement.

  public TrainDBPreparedStatement(TrainDBConnection connection, String sql, int rsType,
                                  int rsConcurrency, int rsHoldability) {
    super(connection, rsType, rsConcurrency, rsHoldability);

    this.connection = connection;
    this.sql = sql;
    this.preparedParameters = new ParameterList(sql);
  }

  public boolean executeWithFlags(int flags) throws SQLException {
		/*
		try {
			checkClosed();

			execute(this.sql, preparedParameters, flags);

			synchronized (this) {
				checkClosed();
				return (result != null && result.getResultSet() != null);
			}
		} finally {
			defaultTimeZone = null;
		}
		*/

    checkClosed();

    StatementResultHandler handler = new StatementResultHandler();

    synchronized (this) {
      result = null;
    }

    System.out.println("==> Execute Start");
    connection.getQueryExecutor().execute(sql, preparedParameters, handler);
    System.out.println("==> Execute End");

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

  private void bindString(int paramIndex, String s, int oid) throws SQLException {
    preparedParameters.setStringParameter(paramIndex, s, oid);
  }

  protected void bindInt(int paramIndex, int x) throws SQLException {
    preparedParameters.setIntParameter(paramIndex, x);
  }

  protected void bindLiteral(int paramIndex, String s, int oid) throws SQLException {
    preparedParameters.setLiteralParameter(paramIndex, s, oid);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new TrainDBJdbcException(
        "Can''t use query methods that take a query string on a PreparedStatement.",
        TrainDBState.WRONG_OBJECT_TYPE);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    if (!executeWithFlags(0)) {
      throw new TrainDBJdbcException("No results were returned by the query.",
          TrainDBState.NO_DATA);
    }

    return getSingleResultSet();
  }

  @Override
  public int executeUpdate() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    checkClosed();
	    /*
	    if (connection.binaryTransferSend(Oid.INT2)) {
	      byte[] val = new byte[2];
	      ByteConverter.int2(val, 0, x);
	      bindBytes(parameterIndex, val, Oid.INT2);
	      return;
	    }
	    */

    bindLiteral(parameterIndex, Integer.toString(x), Types.SMALLINT);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    checkClosed();
	    /*
	    if (connection.binaryTransferSend(Oid.INT4)) {
	      byte[] val = new byte[4];
	      ByteConverter.int4(val, 0, x);
	      bindBytes(parameterIndex, val, Oid.INT4);
	      return;
	    }
	    */

    bindInt(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    checkClosed();
		/*
	    if (connection.binaryTransferSend(Oid.INT8)) {
	      byte[] val = new byte[8];
	      ByteConverter.int8(val, 0, x);
	      bindBytes(parameterIndex, val, Oid.INT8);
	      return;
	    }
	    */

    bindLiteral(parameterIndex, Long.toString(x), Types.BIGINT);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    checkClosed();
		/*
	    if (connection.binaryTransferSend(Oid.FLOAT4)) {
	      byte[] val = new byte[4];
	      ByteConverter.float4(val, 0, x);
	      bindBytes(parameterIndex, val, Oid.FLOAT4);
	      return;
	    }
	    */
    bindLiteral(parameterIndex, Float.toString(x), Types.FLOAT);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    checkClosed();
	    /*
	    if (connection.binaryTransferSend(Oid.FLOAT8)) {
	      byte[] val = new byte[8];
	      ByteConverter.float8(val, 0, x);
	      bindBytes(parameterIndex, val, Oid.FLOAT8);
	      return;
	    }
	    */

    bindLiteral(parameterIndex, Double.toString(x), Types.DOUBLE);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    checkClosed();

    if (x == null) {
      preparedParameters.setNull(parameterIndex, Types.VARCHAR);
    } else {
      bindString(parameterIndex, x, Types.VARCHAR);
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    checkClosed();

    if (null == x) {
      setNull(parameterIndex, Types.VARBINARY);
      return;
    }
    byte[] copy = new byte[x.length];
    System.arraycopy(x, 0, copy, 0, x.length);
    preparedParameters.setBytea(parameterIndex, copy, 0, x.length);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void clearParameters() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    checkClosed();

    if (x == null) {
      setNull(parameterIndex, Types.OTHER);
    } else if (x instanceof SQLXML) {
      setSQLXML(parameterIndex, (SQLXML) x);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof BigDecimal) {
      setBigDecimal(parameterIndex, (BigDecimal) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, (Short) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof byte[] || x instanceof Byte[]) {
      setBytes(parameterIndex, (byte[]) x);
    } else if (x instanceof java.sql.Date) {
      setDate(parameterIndex, (java.sql.Date) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof Byte) {
      setByte(parameterIndex, (Byte) x);
    } else if (x instanceof Blob) {
      setBlob(parameterIndex, (Blob) x);
    } else if (x instanceof Clob) {
      setClob(parameterIndex, (Clob) x);
    } else if (x instanceof Array) {
      setArray(parameterIndex, (Array) x);
    } else if (x instanceof Character) {
      setString(parameterIndex, ((Character) x).toString());
    } else {
      // Can't infer a type.
      throw new TrainDBJdbcException(
          "Can''t infer the SQL type to use for an instance of " + x.getClass().getName() + ". "
              + "Use setObject() with an explicit Types value to specify the type to use.",
          TrainDBState.INVALID_PARAMETER_TYPE);
    }
  }

  @Override
  public boolean execute() throws SQLException {
    return executeWithFlags(0);
  }

  @Override
  public void addBatch() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }
}
