/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package traindb.jdbc;

import java.sql.Connection;
import java.sql.JDBCType;
import java.text.MessageFormat;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import traindb.jdbc.core.Field;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public class TrainDBResultSetMetaData implements ResultSetMetaData {
  protected final Connection connection;
  protected final Field[] fields;

  // private boolean fieldInfoFetched;

  /**
   * Initialise for a result with a tuple set and a field descriptor set
   *
   * @param connection the connection to retrieve metadata
   * @param fields the array of field descriptors
   */
  public TrainDBResultSetMetaData(Connection connection, Field[] fields) {
    this.connection = connection;
    this.fields = fields;
    // this.fieldInfoFetched = false;
  }

  public int getColumnCount() throws SQLException {
    return fields.length;
  }

  /**
   * {@inheritDoc}
   *
   * <p>It is believed that PostgreSQL does not support this feature.
   *
   * @param column the first column is 1, the second is 2...
   * @return true if so
   * @exception SQLException if a database access error occurs
   */
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Does a column's case matter? ASSUMPTION: Any field that is not obviously case insensitive is
   * assumed to be case sensitive
   *
   * @param column the first column is 1, the second is 2...
   * @return true if so
   * @exception SQLException if a database access error occurs
   */
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Can the column be used in a WHERE clause? Basically for this, I split the functions into two
   * types: recognised types (which are always useable), and OTHER types (which may or may not be
   * useable). The OTHER types, for now, I will assume they are useable. We should really query the
   * catalog to see if they are useable.
   *
   * @param column the first column is 1, the second is 2...
   * @return true if they can be used in a WHERE clause
   * @exception SQLException if a database access error occurs
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Is the column a cash value? 6.1 introduced the cash/money type, which haven't been incorporated
   * as of 970414, so I just check the type name for both 'cash' and 'money'
   *
   * @param column the first column is 1, the second is 2...
   * @return true if its a cash column
   * @exception SQLException if a database access error occurs
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Is the column a signed number? In PostgreSQL, all numbers are signed, so this is trivial.
   * However, strings are not signed (duh!)
   *
   * @param column the first column is 1, the second is 2...
   * @return true if so
   * @exception SQLException if a database access error occurs
   */
  public boolean isSigned(int column) throws SQLException {
    Field field = getField(column);
    switch (field.type) {
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.NUMERIC:
        return true;
      default:
        return false;
    }
  }

  public int getColumnDisplaySize(int column) throws SQLException {
    int unknownLength = 40;
    Field field = getField(column);
    switch (field.type) {
      case Types.SMALLINT:
        return 6; // -32768 to +32767
      case Types.INTEGER:
        return 11; // -2147483648 to +2147483647
      case Types.BIGINT:
        return 20; // -9223372036854775808 to +9223372036854775807
      case Types.FLOAT:
        // varies based upon the extra_float_digits GUC.
        // These values are for the longest possible length.
        return 15; // sign + 9 digits + decimal point + e + sign + 2 digits
      case Types.DOUBLE:
        return 25; // sign + 18 digits + decimal point + e + sign + 3 digits
      case Types.CHAR:
      case Types.BOOLEAN:
        return 1;
      case Types.DATE:
        return 13; // "4713-01-01 BC" to "01/01/4713 BC" - "31/12/32767"
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        // Calculate the number of decimal digits + the decimal point.
        int secondSize;
        /*
        switch (typmod) {
          case -1:
            secondSize = 6 + 1;
            break;
          case 0:
            secondSize = 0;
            break;
          case 1:
            // Bizarrely SELECT '0:0:0.1'::time(1); returns 2 digits.
            secondSize = 2 + 1;
            break;
          default:
            secondSize = typmod + 1;
            break;
        }
         */
        secondSize = 0;

        // We assume the worst case scenario for all of these.
        // time = '00:00:00' = 8
        // date = '5874897-12-31' = 13 (although at large values second precision is lost)
        // date = '294276-11-20' = 12 --enable-integer-datetimes
        // zone = '+11:30' = 6;
        switch (field.type) {
          case Types.TIME:
            return 8 + secondSize;
          case Types.TIME_WITH_TIMEZONE:
            return 8 + secondSize + 6;
          case Types.TIMESTAMP:
            return 13 + 1 + 8 + secondSize;
          case Types.TIMESTAMP_WITH_TIMEZONE:
            return 13 + 1 + 8 + secondSize + 6;
        }
      case Types.VARCHAR:
        /*
        if (typmod == -1) {
          return unknownLength;
        }
        return typmod - 4;
         */
        return unknownLength;
      case Types.NUMERIC:
        /*
        if (typmod == -1) {
          return 131089; // SELECT LENGTH(pow(10::numeric,131071)); 131071 = 2^17-1
        }
        int precision = (typmod - 4 >> 16) & 0xffff;
        int scale = (typmod - 4) & 0xffff;
        // sign + digits + decimal point (only if we have nonzero scale)
        return 1 + precision + (scale != 0 ? 1 : 0);
         */
        return 131089;
      default:
        return unknownLength;
    }
  }

  public String getColumnLabel(int column) throws SQLException {
    Field field = getField(column);
    return field.name;
  }

  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  public String getBaseColumnName(int column) throws SQLException {
    return "";
  }

  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  public String getBaseSchemaName(int column) throws SQLException {
    return "";
  }

  public int getPrecision(int column) throws SQLException {
    int unknownLength = 40;
    Field field = getField(column);
    switch (field.type) {
      case Types.SMALLINT:
        return 5;
      case Types.INTEGER:
        return 10;
      case Types.BIGINT:
        return 19;
      case Types.FLOAT:
        // For float4 and float8, we can normally only get 6 and 15
        // significant digits out, but extra_float_digits may raise
        // that number by up to two digits.
        return 8;
      case Types.DOUBLE:
        return 17;
      case Types.NUMERIC:
        /*
        if (typmod == -1) {
          return 0;
        }
        return ((typmod - 4) & 0xFFFF0000) >> 16;
         */
        return 0;
      case Types.CHAR:
      case Types.BOOLEAN:
        return 1;
      case Types.VARCHAR:
        /*
        if (typmod == -1) {
          return unknownLength;
        }
        return typmod - 4;
         */
        return unknownLength;

      // datetime types get the
      // "length in characters of the String representation"
      case Types.DATE:
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return getColumnDisplaySize(column);

      default:
        return unknownLength;
    }

  }

  public int getScale(int column) throws SQLException {
    Field field = getField(column);
    switch (field.type) {
      case Types.FLOAT:
        return 8;
      case Types.DOUBLE:
        return 17;
      case Types.NUMERIC:
        /*
        if (field.mod == -1) {
          return 0;
        }
        return (field.mod - 4) & 0xFFFF;
         */
        return 0;
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        /*
        if (field.mod == -1) {
          return 6;
        }
        return field.mod;
         */
        return 6;
      default:
        return 0;
    }
  }

  public String getTableName(int column) throws SQLException {
    return getBaseTableName(column);
  }

  public String getBaseTableName(int column) throws SQLException {
    return "";
  }

  /**
   * {@inheritDoc}
   *
   * <p>As with getSchemaName(), we can say that if
   * getTableName() returns n/a, then we can too - otherwise, we need to work on it.
   *
   * @param column the first column is 1, the second is 2...
   * @return catalog name, or "" if not applicable
   * @exception SQLException if a database access error occurs
   */
  public String getCatalogName(int column) throws SQLException {
    return "";
  }

  public int getColumnType(int column) throws SQLException {
    return getField(column).type;
  }

  public int getFormat(int column) throws SQLException {
    return getField(column).format;
  }

  public String getColumnTypeName(int column) throws SQLException {
    return JDBCType.valueOf(getField(column).type).getName();
  }

  /**
   * {@inheritDoc}
   *
   * <p>In reality, we would have to check the GRANT/REVOKE
   * stuff for this to be effective, and I haven't really looked into that yet, so this will get
   * re-visited.
   *
   * @param column the first column is 1, the second is 2, etc.*
   * @return true if so*
   * @exception SQLException if a database access error occurs
   */
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>In reality have to check
   * the GRANT/REVOKE stuff, which I haven't worked with as yet. However, if it isn't ReadOnly, then
   * it is obviously writable.
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return true if so
   * @exception SQLException if a database access error occurs
   */
  public boolean isWritable(int column) throws SQLException {
    return !isReadOnly(column);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Hmmm...this is a bad one, since the two
   * preceding functions have not been really defined. I cannot tell is the short answer. I thus
   * return isWritable() just to give us an idea.
   *
   * @param column the first column is 1, the second is 2, etc..
   * @return true if so
   * @exception SQLException if a database access error occurs
   */
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    Field field = getField(column);
    switch (field.type) {
      case Types.ARRAY:
        return ("java.sql.Array");
      default:
        return ("java.lang.Object");
    }
  }

  // ********************************************************
  // END OF PUBLIC INTERFACE
  // ********************************************************

  /**
   * For several routines in this package, we need to convert a columnIndex into a Field[]
   * descriptor. Rather than do the same code several times, here it is.
   *
   * @param columnIndex the first column is 1, the second is 2...
   * @return the Field description
   * @exception SQLException if a database access error occurs
   */
  protected Field getField(int columnIndex) throws SQLException {
    if (columnIndex < 1 || columnIndex > fields.length) {
      throw new TrainDBJdbcException(
          MessageFormat.format("The column index is out of range: {0}, number of columns: {1}.",
              columnIndex, fields.length),
          TrainDBState.INVALID_PARAMETER_VALUE);
    }
    return fields[columnIndex - 1];
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }
}
