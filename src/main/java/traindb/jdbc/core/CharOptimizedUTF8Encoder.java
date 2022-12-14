package traindb.jdbc.core;

import java.io.IOException;

/**
 * UTF-8 encoder which validates input and is optimized for jdk 8 and lower
 * where {@code String} objects are backed by {@code char[]}.
 * 
 * @author Brett Okken
 */
final class CharOptimizedUTF8Encoder extends OptimizedUTF8Encoder {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String decode(byte[] encodedString, int offset, int length) throws IOException {
		return charDecode(encodedString, offset, length);
	}
}