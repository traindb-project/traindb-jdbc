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
import java.nio.charset.StandardCharsets;

/**
 * UTF-8 encoder which validates input and is optimized for jdk 9+ where
 * {@code String} objects are backed by {@code byte[]}.
 *
 * @author Brett Okken
 */
final class ByteOptimizedUTF8Encoder extends OptimizedUTF8Encoder {

  /**
   * {@inheritDoc}
   */
  @Override
  public String decode(byte[] encodedString, int offset, int length) throws IOException {
    // for very short strings going straight to chars is up to 30% faster
    if (length <= 32) {
      return charDecode(encodedString, offset, length);
    }
    for (int i = offset, j = offset + length; i < j; ++i) {
      // bytes are signed values. all ascii values are positive
      if (encodedString[i] < 0) {
        return slowDecode(encodedString, offset, length, i);
      }
    }
    // we have confirmed all chars are ascii, give java that hint
    return new String(encodedString, offset, length, StandardCharsets.US_ASCII);
  }

  /**
   * Decodes to {@code char[]} in presence of non-ascii values after first copying
   * all known ascii chars directly from {@code byte[]} to {@code char[]}.
   */
  private synchronized String slowDecode(byte[] encodedString, int offset, int length, int curIdx)
      throws IOException {
    final char[] chars = getCharArray(length);
    int out = 0;
    for (int i = offset; i < curIdx; ++i) {
      chars[out++] = (char) encodedString[i];
    }
    return decodeToChars(encodedString, curIdx, length - (curIdx - offset), chars, out);
  }
}