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

package traindb.jdbc.util;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class TrainDBJdbcException extends SQLException {
  private ServerErrorMessage _serverError;

  public TrainDBJdbcException(String msg, TrainDBState state, Throwable cause) {
    super(msg, state == null ? null : state.getState());
    initCause(cause);
  }

  public TrainDBJdbcException(String msg) {
    this(msg, null, null);
  }

  public TrainDBJdbcException(String msg, TrainDBState state) {
    this(msg, state, null);
    // super(msg, state == null ? null : state.getState());
  }

  public TrainDBJdbcException(ServerErrorMessage serverError) {
    this(serverError.toString(), new TrainDBState(serverError.getSQLState()));
    _serverError = serverError;
  }

  public ServerErrorMessage getServerErrorMessage() {
    return _serverError;
  }
}
