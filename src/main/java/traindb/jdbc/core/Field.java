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

public class Field {
  public static final int TEXT_FORMAT = 0;
  public static final int BINARY_FORMAT = 1;
  public final int type;
  public final int size;
  public final int format;
  public String name;

  public Field(String name, int type, int size, int format) {
    this.name = name;
    this.type = type;
    this.size = size;
    this.format = format;
  }

}
