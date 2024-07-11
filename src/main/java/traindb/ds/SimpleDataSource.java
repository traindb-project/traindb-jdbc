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

package traindb.ds;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import javax.sql.DataSource;
import traindb.ds.common.BaseDataSource;

/**
 * A simple DataSource without connection pooling.
 */
public class SimpleDataSource extends BaseDataSource implements DataSource, Serializable {
  public String getDescription() {
    return "TrainDB Simple DataSource";
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeBaseObject(out);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    readBaseObject(in);
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
