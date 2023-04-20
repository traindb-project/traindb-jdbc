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
  public static QueryExecutor openConnection(String url, Properties info) throws SQLException {
    String protoName = TrainDBProperty.PROTOCOL_VERSION.get(info);

    ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
    QueryExecutor queryExecutor = connectionFactory.openConnectionImpl(url, info);

    if (queryExecutor != null) {
      return queryExecutor;
    }

    throw new TrainDBJdbcException(
        MessageFormat.format("A connection could not be made using the requested protocol {0}.",
            protoName), TrainDBState.CONNECTION_UNABLE_TO_CONNECT);
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
