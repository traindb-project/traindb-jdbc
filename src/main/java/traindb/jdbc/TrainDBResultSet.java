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
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.jdbc.core.Field;
import traindb.jdbc.core.ResultCursor;
import traindb.jdbc.core.Tuple;
import traindb.jdbc.util.ByteConverter;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

// Borrowed from org.postgresql.jdbc.PgResultSet
public class TrainDBResultSet implements ResultSet {
  private static final BigInteger SHORTMAX = new BigInteger(Short.toString(Short.MAX_VALUE));
  private static final BigInteger SHORTMIN = new BigInteger(Short.toString(Short.MIN_VALUE));
  private static final BigInteger INTMAX = new BigInteger(Integer.toString(Integer.MAX_VALUE));
  private static final BigInteger INTMIN = new BigInteger(Integer.toString(Integer.MIN_VALUE));
  private static final BigInteger LONGMAX = new BigInteger(Long.toString(Long.MAX_VALUE));
  private static final BigInteger LONGMIN = new BigInteger(Long.toString(Long.MIN_VALUE));
  private static final float LONG_MAX_FLOAT = StrictMath.nextDown(Long.MAX_VALUE);
  private static final float LONG_MIN_FLOAT = StrictMath.nextUp(Long.MIN_VALUE);
  private static final double LONG_MAX_DOUBLE = StrictMath.nextDown((double) Long.MAX_VALUE);
  private static final double LONG_MIN_DOUBLE = StrictMath.nextUp((double) Long.MIN_VALUE);
  private final String originalQuery;
  private final Connection connection;
  private final TrainDBStatement statement;
  private final List<Tuple> rows;
  private final int maxRows; // Maximum rows in this resultset (might be 0).
  private final int maxFieldSize; // Maximum field size in this resultset (might be 0).
  private final int resultsetconcurrency;
  private final Field[] fields;
  private int currentRow = -1; // Index into 'rows' of our currrent row (0-based)
  private int rowOffset; // Offset of row 0 in the actual resultset
  private Tuple thisRow;
  private Tuple rowBuffer = null; // updateable rowbuffer
  private boolean wasNullFlag = false;
  private boolean onInsertRow = false;

  // Speed up findColumn by caching lookups
  private @Nullable Map<String, Integer> columnNameIndexMap;

  private @Nullable ResultSetMetaData rsMetaData;

  public TrainDBResultSet(String originalQuery, TrainDBStatement statement, Field[] fields,
                          List<Tuple> tuples, @Nullable ResultCursor cursor, int maxRows,
                          int maxFieldSize, int resultSetType,
                          int resultSetConcurrency, int resultSetHoldability, boolean adaptiveFetch)
      throws SQLException {
    if (tuples == null) {
      throw new NullPointerException("tuples must be non-null");
    }
    if (fields == null) {
      throw new NullPointerException("fields must be non-null");
    }

    this.originalQuery = originalQuery;
    this.connection = statement.getConnection();
    this.statement = statement;
    this.fields = fields;
    this.rows = tuples;
    // this.cursor = cursor;
    this.maxRows = maxRows;
    this.maxFieldSize = maxFieldSize;
    // this.resultsettype = rsType;
    this.resultsetconcurrency = resultSetConcurrency;
    // this.adaptiveFetch = adaptiveFetch;

    // Constructor doesn't have fetch size and can't be sure if fetch size was used so initial value would be the number of rows
    // this.lastUsedFetchSize = tuples.size();
  }

  public static short toShort(@Nullable String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Short.parseShort(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();
          int gt = i.compareTo(SHORTMAX);
          int lt = i.compareTo(SHORTMIN);

          if (gt > 0 || lt < 0) {
            throw new TrainDBJdbcException(
                MessageFormat.format("Bad value for type {0} : {1}", "short", s),
                TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.shortValue();

        } catch (NumberFormatException ne) {
          throw new TrainDBJdbcException(
              MessageFormat.format("Bad value for type {0} : {1}", "short", s),
              TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  public static int toInt(@Nullable String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();

          int gt = i.compareTo(INTMAX);
          int lt = i.compareTo(INTMIN);

          if (gt > 0 || lt < 0) {
            throw new TrainDBJdbcException(
                MessageFormat.format("Bad value for type {0} : {1}", "int", s),
                TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.intValue();

        } catch (NumberFormatException ne) {
          throw new TrainDBJdbcException(
              MessageFormat.format("Bad value for type {0} : {1}", "int", s),
              TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  public static long toLong(@Nullable String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Long.parseLong(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();
          int gt = i.compareTo(LONGMAX);
          int lt = i.compareTo(LONGMIN);

          if (gt > 0 || lt < 0) {
            throw new TrainDBJdbcException(
                MessageFormat.format("Bad value for type {0} : {1}", "long", s),
                TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.longValue();
        } catch (NumberFormatException ne) {
          throw new TrainDBJdbcException(
              MessageFormat.format("Bad value for type {0} : {1}", "long", s),
              TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  public static float toFloat(@Nullable String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Float.parseFloat(s);
      } catch (NumberFormatException e) {
        throw new TrainDBJdbcException(
            MessageFormat.format("Bad value for type {0} : {1}", "float", s),
            TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
    return 0; // SQL NULL
  }

  public static double toDouble(@Nullable String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Double.parseDouble(s);
      } catch (NumberFormatException e) {
        throw new TrainDBJdbcException(
            MessageFormat.format("Bad value for type {0} : {1}", "double", s),
            TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
    return 0; // SQL NULL
  }

  protected ResultSetMetaData createMetaData() throws SQLException {
    return new TrainDBResultSetMetaData(connection, fields);
  }

  byte[] getRawValue(@Positive int column) throws SQLException {
    checkClosed();

    if (thisRow == null) {
      throw new TrainDBJdbcException(
          "ResultSet not positioned properly, perhaps you need to call next.",
          TrainDBState.INVALID_CURSOR_STATE);
    }

    checkColumnIndex(column);
    byte[] bytes = thisRow.get(column - 1);
    wasNullFlag = bytes == null;
    return bytes;
  }

  private void initRowBuffer() {
    thisRow = rows.get(currentRow);
    // We only need a copy of the current row if we're going to
    // modify it via an updatable resultset.
    if (resultsetconcurrency == ResultSet.CONCUR_UPDATABLE) {
      rowBuffer = thisRow.updateableCopy();
    } else {
      rowBuffer = null;
    }
  }

  private void checkColumnIndex(@Positive int column) throws SQLException {
    if (column < 1 || column > fields.length) {
      throw new TrainDBJdbcException(
          MessageFormat.format("The column index is out of range: {0}, number of columns: {1}.",
              column, fields.length), TrainDBState.INVALID_PARAMETER_VALUE);
    }
  }

  private String trimString(@Positive int columnIndex, String string) throws SQLException {
    // we need to trim if maxsize is set and the length is greater than maxsize and the
    // type of this column is a candidate for trimming
		/*
		if (maxFieldSize > 0 && string.length() > maxFieldSize && isColumnTrimmable(columnIndex)) {
			return string.substring(0, maxFieldSize);
		} else {
			return string;
		}
		*/

    return string;
  }

  protected void checkClosed() throws SQLException {
    if (rows == null) {
      throw new TrainDBJdbcException("This ResultSet is closed.", TrainDBState.OBJECT_NOT_IN_STATE);
    }
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
  public boolean next() throws SQLException {
    checkClosed();

    if (onInsertRow) {
      throw new TrainDBJdbcException("Can''t use relative move methods while on the insert row.",
          TrainDBState.INVALID_CURSOR_STATE);
    }

    if (currentRow + 1 >= rows.size()) {
      currentRow = rows.size();
      thisRow = null;
      rowBuffer = null;
      return false; // End of the resultset.
    } else {
      currentRow++;
    }

    initRowBuffer();
    return true;
  }

  @Override
  public void close() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean wasNull() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return null;
    }

    return trimString(columnIndex, new String(value));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int type = fields[col].type;
      if (type == Types.SMALLINT) {
        return ByteConverter.int2(value, 0);
      }
      return (short) readLongValue(value, type, Short.MIN_VALUE, Short.MAX_VALUE, "short");
    }

    return toShort(getString(columnIndex));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int type = fields[col].type;
      if (type == Types.INTEGER) {
        return ByteConverter.int4(value, 0);
      }
      return (int) readLongValue(value, type, Integer.MIN_VALUE, Integer.MAX_VALUE, "int");
    }

    return toInt(getString(columnIndex));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int type = fields[col].type;
      if (type == Types.BIGINT) {
        return ByteConverter.int8(value, 0);
      }
      return readLongValue(value, type, Long.MIN_VALUE, Long.MAX_VALUE, "long");
    }

    return toLong(getString(columnIndex));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int type = fields[col].type;
      if (type == Types.FLOAT) {
        return ByteConverter.float4(value, 0);
      }
      return (float) readDoubleValue(value, type, "float");
    }

    return toFloat(getString(columnIndex));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int type = fields[col].type;
      if (type == Types.DOUBLE) {
        return ByteConverter.float8(value, 0);
      }
      return readDoubleValue(value, type, "double");
    }

    return toDouble(getString(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return null;
    }
    return value;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getTimestamp(columnIndex, null);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
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
  public String getCursorName() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();
    if (rsMetaData == null) {
      rsMetaData = createMetaData();
    }
    return rsMetaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    byte[] value = getRawValue(columnIndex);
    if (value == null) {
      return null;
    }

    Field field = fields[columnIndex - 1];

    // some fields can be null, mainly from those returned by MetaData methods
    if (field == null) {
      wasNullFlag = true;
      return null;
    }

    return internalGetObject(columnIndex, field);
		/*
		Object result = internalGetObject(columnIndex, field);
		if (result != null) {
			return result;
		}

		if (isBinary(columnIndex)) {
			return connection.getObject(getPGType(columnIndex), null, value);
		}
		String stringValue = castNonNull(getString(columnIndex));
		return connection.getObject(getPGType(columnIndex), stringValue, null);
		 */
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  protected @Nullable Object internalGetObject(@Positive int columnIndex, Field field)
      throws SQLException {
    switch (field.type) {
      case Types.BOOLEAN:
        return getBoolean(columnIndex);
      case Types.SQLXML:
        return getSQLXML(columnIndex);
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return getInt(columnIndex);
      case Types.BIGINT:
        return getLong(columnIndex);
      case Types.NUMERIC:
      case Types.DECIMAL:
				/*
				return getNumeric(columnIndex,
						(field.getMod() == -1) ? -1 : ((field.getMod() - 4) & 0xffff), true);
				 */
        return getDouble(columnIndex);
      case Types.REAL:
      case Types.FLOAT:
        return getFloat(columnIndex);
      case Types.DOUBLE:
        return getDouble(columnIndex);
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return getString(columnIndex);
      case Types.DATE:
        return getDate(columnIndex);
      case Types.TIME:
        return getTime(columnIndex);
      case Types.TIMESTAMP:
        return getTimestamp(columnIndex, null);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return getBytes(columnIndex);
      case Types.ARRAY:
        return getArray(columnIndex);
      case Types.CLOB:
        return getClob(columnIndex);
      case Types.BLOB:
        return getBlob(columnIndex);

      default:
        return getString(columnIndex);
    }
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    checkClosed();

    int col = findColumnIndex(columnLabel);
    if (col == 0) {
      throw new TrainDBJdbcException(
          "The column name " + columnLabel + " was not found in this Resultset.",
          TrainDBState.UNDEFINED_COLUMN);
    }
    return col;
  }

  public static Map<String, Integer> createColumnNameIndexMap(Field[] fields) {
    Map<String, Integer> columnNameIndexMap = new HashMap<>(fields.length * 2);
    // The JDBC spec says when you have duplicate columns names,
    // the first one should be returned. So load the map in
    // reverse order so the first ones will overwrite later ones.
    for (int i = fields.length - 1; i >= 0; i--) {
      String columnLabel = fields[i].name;
      columnNameIndexMap.put(columnLabel, i + 1);
    }
    return columnNameIndexMap;
  }

  private int findColumnIndex(String columnName) {
    if (columnNameIndexMap == null) {
      columnNameIndexMap = createColumnNameIndexMap(fields);
    }

    Integer index = columnNameIndexMap.get(columnName);
    if (index != null) {
      return index;
    }

    return 0;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    return ((rowOffset + currentRow) < 0 && !rows.isEmpty());
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    final int rows_size = rows.size();

    if (rowOffset + rows_size == 0) {
      return false;
    }

    return (currentRow >= rows_size);
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    final int rows_size = rows.size();
    if (rowOffset + rows_size == 0) {
      return false;
    }

    return ((rowOffset + currentRow) == 0);
  }

  @Override
  public boolean isLast() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {
    // checkScrollable();

    if (!rows.isEmpty()) {
      currentRow = -1;
    }

    onInsertRow = false;
    thisRow = null;
    rowBuffer = null;
  }

  @Override
  public void afterLast() throws SQLException {
    // checkScrollable();

    final int rows_size = rows.size();

    if (rows_size > 0) {
      currentRow = rows_size;
    }

    onInsertRow = false;
    thisRow = null;
    rowBuffer = null;
  }

  @Override
  public boolean first() throws SQLException {
    // checkScrollable();

    if (rows.size() <= 0) {
      return false;
    }

    currentRow = 0;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }

  @Override
  public boolean last() throws SQLException {
    // checkScrollable();

    List<Tuple> rows = this.rows;
    final int rows_size = rows.size();
    if (rows_size <= 0) {
      return false;
    }

    currentRow = rows_size - 1;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();

    if (onInsertRow) {
      return 0;
    }

    final int rows_size = rows.size();

    if (currentRow < 0 || currentRow >= rows_size) {
      return 0;
    }

    return rowOffset + currentRow + 1;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    // checkScrollable();

    if (onInsertRow) {
      throw new TrainDBJdbcException("Can''t use relative move methods while on the insert row.",
          TrainDBState.INVALID_CURSOR_STATE);
    }

    // have to add 1 since absolute expects a 1-based index
    int index = currentRow + 1 + rows;
    if (index < 0) {
      beforeFirst();
      return false;
    }
    return absolute(index);
  }

  @Override
  public boolean previous() throws SQLException {
    // checkScrollable();

    if (onInsertRow) {
      throw new TrainDBJdbcException("Can''t use relative move methods while on the insert row.",
          TrainDBState.INVALID_CURSOR_STATE);
    }

    if (currentRow - 1 < 0) {
      currentRow = -1;
      thisRow = null;
      rowBuffer = null;
      return false;
    } else {
      currentRow--;
    }
    initRowBuffer();
    return true;
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
  public int getType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void insertRow() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateRow() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteRow() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void refreshRow() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void moveToInsertRow() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public Statement getStatement() throws SQLException {
    checkClosed();
    return statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    Timestamp ts = Timestamp.valueOf(getString(columnIndex));
    return Timestamp.valueOf(getString(columnIndex));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  // ----------------- Formatting Methods -------------------

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  protected boolean isBinary(@Positive int column) {
    return fields[column - 1].format == Field.BINARY_FORMAT;
  }

  private long readLongValue(byte[] bytes, int type, long minVal, long maxVal, String targetType)
      throws
      TrainDBJdbcException {
    long val;
    // currently implemented binary encoded fields
    switch (type) {
      case Types.SMALLINT:
        val = ByteConverter.int2(bytes, 0);
        break;
      case Types.INTEGER:
        val = ByteConverter.int4(bytes, 0);
        break;
      case Types.BIGINT:
        val = ByteConverter.int8(bytes, 0);
        break;
      case Types.FLOAT:
        float f = ByteConverter.float4(bytes, 0);
        // for float values we know to be within values of long, just cast directly to long
        if (f <= LONG_MAX_FLOAT && f >= LONG_MIN_FLOAT) {
          val = (long) f;
        } else {
          throw new TrainDBJdbcException(
              MessageFormat.format("Bad value for type {0} : {1}", targetType, f),
              TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        break;
      case Types.DOUBLE:
        double d = ByteConverter.float8(bytes, 0);
        // for double values within the values of a long, just directly cast to long
        if (d <= LONG_MAX_DOUBLE && d >= LONG_MIN_DOUBLE) {
          val = (long) d;
        } else {
          throw new TrainDBJdbcException(
              MessageFormat.format("Bad value for type {0} : {1}", targetType, d),
              TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
        break;
      case Types.NUMERIC:
        Number num = ByteConverter.numeric(bytes);
        BigInteger i = ((BigDecimal) num).toBigInteger();
        int gt = i.compareTo(LONGMAX);
        int lt = i.compareTo(LONGMIN);

        if (gt > 0 || lt < 0) {
          throw new TrainDBJdbcException(
              MessageFormat.format("Bad value for type {0} : {1}", "long", num),
              TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
        } else {
          val = num.longValue();
        }
        break;
      default:
        throw new TrainDBJdbcException(
            MessageFormat.format("Cannot convert the column of type {0} to requested type {1}.",
                type, targetType),
            TrainDBState.DATA_TYPE_MISMATCH);
    }
    if (val < minVal || val > maxVal) {
      throw new TrainDBJdbcException(
          MessageFormat.format("Bad value for type {0} : {1}", targetType, val),
          TrainDBState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
    return val;
  }

  private double readDoubleValue(byte[] bytes, int type, String targetType) throws
      TrainDBJdbcException {
    // currently implemented binary encoded fields
    switch (type) {
      case Types.SMALLINT:
        return ByteConverter.int2(bytes, 0);
      case Types.INTEGER:
        return ByteConverter.int4(bytes, 0);
      case Types.BIGINT:
        // might not fit but there still should be no overflow checking
        return ByteConverter.int8(bytes, 0);
      case Types.FLOAT:
        return ByteConverter.float4(bytes, 0);
      case Types.DOUBLE:
        return ByteConverter.float8(bytes, 0);
      case Types.NUMERIC:
        return ByteConverter.numeric(bytes).doubleValue();
    }
    throw new TrainDBJdbcException(
        MessageFormat.format("Cannot convert the column of type {0} to requested type {1}.",
            JDBCType.valueOf(type).getName(), targetType), TrainDBState.DATA_TYPE_MISMATCH);
  }

}
