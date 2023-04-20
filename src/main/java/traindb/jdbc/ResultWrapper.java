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

import java.sql.ResultSet;

public class ResultWrapper {
  private final ResultSet rs;
  private final long updateCount;
  private final long insertOID;
  private ResultWrapper next;

  public ResultWrapper(ResultSet rs) {
    this.rs = rs;
    this.updateCount = -1;
    this.insertOID = -1;
  }

  public ResultWrapper(long updateCount, long insertOID) {
    this.rs = null;
    this.updateCount = updateCount;
    this.insertOID = insertOID;
  }

  public ResultSet getResultSet() {
    return rs;
  }

  public long getUpdateCount() {
    return updateCount;
  }

  public long getInsertOID() {
    return insertOID;
  }

  public ResultWrapper getNext() {
    return next;
  }

  public void append(ResultWrapper newResult) {
    ResultWrapper tail = this;

    while (tail.next != null) {
      tail = tail.next;
    }

    tail.next = newResult;
  }
}
