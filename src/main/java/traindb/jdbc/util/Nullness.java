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

import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/**
 * The methods in this class allow to cast nullable reference to a non-nullable one.
 * This is an internal class, and it is not meant to be used as a public API.
 */
@SuppressWarnings({"cast.unsafe", "NullableProblems", "contracts.postcondition"})
public class Nullness {
  @Pure
  public static @EnsuresNonNull("#1") <T extends @Nullable Object> @NonNull T castNonNull(
      @Nullable T ref) {
    assert ref != null : "Misuse of castNonNull: called with a null argument";
    return (@NonNull T) ref;
  }

  @Pure
  public static @EnsuresNonNull("#1") <T extends @Nullable Object> @NonNull T castNonNull(
      @Nullable T ref, String message) {
    assert ref != null : "Misuse of castNonNull: called with a null argument " + message;
    return (@NonNull T) ref;
  }
}
