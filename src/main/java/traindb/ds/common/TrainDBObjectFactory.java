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

package traindb.ds.common;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.ds.SimpleDataSource;

public class TrainDBObjectFactory implements ObjectFactory {
  /**
   * Dereferences a DataSource. Other types of references are ignored.
   */
  public @Nullable Object getObjectInstance(Object obj, Name name, Context nameCtx,
                                            Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    if (className.equals("traindb.ds.SimpleDataSource")) {
      return loadSimpleDataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadSimpleDataSource(Reference ref) {
    SimpleDataSource ds = new SimpleDataSource();
    return loadBaseDataSource(ds, ref);
  }

  protected Object loadBaseDataSource(BaseDataSource ds, Reference ref) {
    ds.setFromReference(ref);

    return ds;
  }

  protected @Nullable String getProperty(Reference ref, String s) {
    RefAddr addr = ref.get(s);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

}
