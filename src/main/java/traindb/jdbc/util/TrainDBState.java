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

import java.io.Serializable;

@SuppressWarnings("serial")
public class TrainDBState implements Serializable {
  // begin constant state codes
  public final static TrainDBState UNKNOWN_STATE = new TrainDBState("");
  public final static TrainDBState TOO_MANY_RESULTS = new TrainDBState("0100E");
  public final static TrainDBState NO_DATA = new TrainDBState("02000");
  public final static TrainDBState INVALID_PARAMETER_TYPE = new TrainDBState("07006");
  /**
   * We could establish a connection with the server for unknown reasons.
   * Could be a network problem.
   */
  public final static TrainDBState CONNECTION_UNABLE_TO_CONNECT = new TrainDBState("08001");
  public final static TrainDBState CONNECTION_DOES_NOT_EXIST = new TrainDBState("08003");
  /**
   * The server rejected our connection attempt.  Usually an authentication
   * failure, but could be a configuration error like asking for a SSL
   * connection with a server that wasn't built with SSL support.
   */
  public final static TrainDBState CONNECTION_REJECTED = new TrainDBState("08004");
  /**
   * After a connection has been established, it went bad.
   */
  public final static TrainDBState CONNECTION_FAILURE = new TrainDBState("08006");
  public final static TrainDBState CONNECTION_FAILURE_DURING_TRANSACTION =
      new TrainDBState("08007");
  /**
   * The server sent us a response the driver was not prepared for and
   * is either bizarre datastream corruption, a driver bug, or
   * a protocol violation on the server's part.
   */
  public final static TrainDBState PROTOCOL_VIOLATION = new TrainDBState("08P01");
  public final static TrainDBState COMMUNICATION_ERROR = new TrainDBState("08S01");
  public final static TrainDBState NOT_IMPLEMENTED = new TrainDBState("0A000");
  public final static TrainDBState DATA_ERROR = new TrainDBState("22000");
  public final static TrainDBState NUMERIC_VALUE_OUT_OF_RANGE = new TrainDBState("22003");
  public final static TrainDBState BAD_DATETIME_FORMAT = new TrainDBState("22007");
  public final static TrainDBState DATETIME_OVERFLOW = new TrainDBState("22008");
  public final static TrainDBState MOST_SPECIFIC_TYPE_DOES_NOT_MATCH = new TrainDBState("2200G");
  public final static TrainDBState INVALID_PARAMETER_VALUE = new TrainDBState("22023");
  public final static TrainDBState INVALID_CURSOR_STATE = new TrainDBState("24000");
  public final static TrainDBState TRANSACTION_STATE_INVALID = new TrainDBState("25000");
  public final static TrainDBState ACTIVE_SQL_TRANSACTION = new TrainDBState("25001");
  public final static TrainDBState NO_ACTIVE_SQL_TRANSACTION = new TrainDBState("25P01");
  public final static TrainDBState STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL =
      new TrainDBState("2F003");
  public final static TrainDBState INVALID_SAVEPOINT_SPECIFICATION = new TrainDBState("3B000");
  public final static TrainDBState SYNTAX_ERROR = new TrainDBState("42601");
  public final static TrainDBState UNDEFINED_COLUMN = new TrainDBState("42703");
  public final static TrainDBState UNDEFINED_OBJECT = new TrainDBState("42704");
  public final static TrainDBState WRONG_OBJECT_TYPE = new TrainDBState("42809");
  public final static TrainDBState NUMERIC_CONSTANT_OUT_OF_RANGE = new TrainDBState("42820");
  public final static TrainDBState DATA_TYPE_MISMATCH = new TrainDBState("42821");
  public final static TrainDBState UNDEFINED_FUNCTION = new TrainDBState("42883");
  public final static TrainDBState INVALID_NAME = new TrainDBState("42602");
  public final static TrainDBState OUT_OF_MEMORY = new TrainDBState("53200");
  public final static TrainDBState OBJECT_NOT_IN_STATE = new TrainDBState("55000");
  public final static TrainDBState SYSTEM_ERROR = new TrainDBState("60000");
  public final static TrainDBState IO_ERROR = new TrainDBState("58030");
  public final static TrainDBState UNEXPECTED_ERROR = new TrainDBState("99999");
  private String state;

  public TrainDBState(String state) {
    this.state = state;
  }

  public String getState() {
    return this.state;
  }
}