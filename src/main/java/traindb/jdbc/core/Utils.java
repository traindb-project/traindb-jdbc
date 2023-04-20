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
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public class Utils {
  /**
   * Turn a bytearray into a printable form, representing each byte in hex.
   *
   * @param data the bytearray to stringize
   * @return a hex-encoded printable representation of {@code data}
   */
  public static String toHexString(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (byte element : data) {
      sb.append(Integer.toHexString((element >> 4) & 15));
      sb.append(Integer.toHexString(element & 15));
    }
    return sb.toString();
  }

  /**
   * Escape the given literal {@code value} and append it to the string builder
   * {@code sbuf}. If {@code sbuf} is {@code null}, a new StringBuilder will be
   * returned. The argument {@code standardConformingStrings} defines whether the
   * backend expects standard-conforming string literals or allows backslash
   * escape sequences.
   *
   * @param sbuf                      the string builder to append to; or
   *                                  {@code null}
   * @param value                     the string value
   * @param standardConformingStrings if standard conforming strings should be
   *                                  used
   * @return the sbuf argument; or a new string builder for sbuf == null
   * @throws SQLException if the string contains a {@code \0} character
   */
  public static StringBuilder escapeLiteral(@Nullable StringBuilder sbuf, String value,
                                            boolean standardConformingStrings) throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuilder((value.length() + 10) / 10 * 11); // Add 10% for escaping.
    }
    doAppendEscapedLiteral(sbuf, value, standardConformingStrings);
    return sbuf;
  }

  /**
   * Common part for {@link #escapeLiteral(StringBuilder, String, boolean)}.
   *
   * @param sbuf                      Either StringBuffer or StringBuilder as we
   *                                  do not expect any IOException to be thrown
   * @param value                     value to append
   * @param standardConformingStrings if standard conforming strings should be
   *                                  used
   */
  private static void doAppendEscapedLiteral(Appendable sbuf, String value,
                                             boolean standardConformingStrings)
      throws SQLException {
    try {
      if (standardConformingStrings) {
        // With standard_conforming_strings on, escape only single-quotes.
        for (int i = 0; i < value.length(); ++i) {
          char ch = value.charAt(i);
          if (ch == '\0') {
            throw new TrainDBJdbcException("Zero bytes may not occur in string parameters.",
                TrainDBState.INVALID_PARAMETER_VALUE);
          }
          if (ch == '\'') {
            sbuf.append('\'');
          }
          sbuf.append(ch);
        }
      } else {
        // With standard_conforming_string off, escape backslashes and
        // single-quotes, but still escape single-quotes by doubling, to
        // avoid a security hazard if the reported value of
        // standard_conforming_strings is incorrect, or an error if
        // backslash_quote is off.
        for (int i = 0; i < value.length(); ++i) {
          char ch = value.charAt(i);
          if (ch == '\0') {
            throw new TrainDBJdbcException("Zero bytes may not occur in string parameters.",
                TrainDBState.INVALID_PARAMETER_VALUE);
          }
          if (ch == '\\' || ch == '\'') {
            sbuf.append(ch);
          }
          sbuf.append(ch);
        }
      }
    } catch (IOException e) {
      throw new TrainDBJdbcException("No IOException expected from StringBuffer or StringBuilder",
          TrainDBState.UNEXPECTED_ERROR, e);
    }
  }

  /**
   * Escape the given identifier {@code value} and append it to the string builder
   * {@code sbuf}. If {@code sbuf} is {@code null}, a new StringBuilder will be
   * returned. This method is different from appendEscapedLiteral in that it
   * includes the quoting required for the identifier while
   * {@link #escapeLiteral(StringBuilder, String, boolean)} does not.
   *
   * @param sbuf  the string builder to append to; or {@code null}
   * @param value the string value
   * @return the sbuf argument; or a new string builder for sbuf == null
   * @throws SQLException if the string contains a {@code \0} character
   */
  public static StringBuilder escapeIdentifier(@Nullable StringBuilder sbuf, String value)
      throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuilder(2 + (value.length() + 10) / 10 * 11); // Add 10% for escaping.
    }
    doAppendEscapedIdentifier(sbuf, value);
    return sbuf;
  }

  /**
   * Common part for appendEscapedIdentifier.
   *
   * @param sbuf  Either StringBuffer or StringBuilder as we do not expect any
   *              IOException to be thrown.
   * @param value value to append
   */
  private static void doAppendEscapedIdentifier(Appendable sbuf, String value) throws SQLException {
    try {
      sbuf.append('"');

      for (int i = 0; i < value.length(); ++i) {
        char ch = value.charAt(i);
        if (ch == '\0') {
          throw new TrainDBJdbcException("Zero bytes may not occur in identifiers.",
              TrainDBState.INVALID_PARAMETER_VALUE);
        }
        if (ch == '"') {
          sbuf.append(ch);
        }
        sbuf.append(ch);
      }

      sbuf.append('"');
    } catch (IOException e) {
      throw new TrainDBJdbcException("No IOException expected from StringBuffer or StringBuilder",
          TrainDBState.UNEXPECTED_ERROR, e);
    }
  }
}
