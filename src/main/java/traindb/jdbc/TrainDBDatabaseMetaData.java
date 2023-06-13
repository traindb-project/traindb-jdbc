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

import java.sql.Connection;
import java.sql.SQLException;

class TrainDBDatabaseMetaData extends AbstractDatabaseMetaData {
  private final TrainDBConnection connection;

  TrainDBDatabaseMetaData(TrainDBConnection conn) {
    this.connection = conn;
  }

  @Override
  public String getURL() throws SQLException {
    return connection.getUrl();
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "TrainDB";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return "unknown version";
  }

  @Override
  public String getDriverName() throws SQLException {
    return "TrainDB JDBC Driver";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return Driver.MAJORVERSION + "." + Driver.MINORVERSION;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException {
    return level == Connection.TRANSACTION_NONE;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

}
