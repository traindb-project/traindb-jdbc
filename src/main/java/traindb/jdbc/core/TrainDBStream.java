package traindb.jdbc.core;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.sql.SQLException;

import java.text.MessageFormat;
import javax.net.SocketFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

import traindb.jdbc.util.HostSpec;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

// stream.java 참고
// pgjdbc/pgjdbc/src/main/java/org/postgresql/core/stream.java 
public class TrainDBStream implements Closeable, Flushable {
	public static final int PACKET_COMMON_STX 							= 0xa5;
	public static final int PACKET_COMMON_ETX 							= 0x7e;
	
	private final SocketFactory socketFactory;
	private final HostSpec hostSpec;
	private Socket connection;
	private Encoding encoding;
	private Writer encodingWriter;

	private VisibleBufferedInputStream input;
	private OutputStream output;
	private byte @Nullable [] streamBuffer;
	
	private final byte[] int2Buf;
	private final byte[] int4Buf;
	
	private long maxResultBuffer = -1;
	private long resultBufferByteCount = 0;
	  
	private int maxRowSizeBytes = -1;

	public TrainDBStream(SocketFactory socketFactory, HostSpec hostSpec, int timeout) throws IOException {
		this.socketFactory = socketFactory;
		this.hostSpec = hostSpec;

		Socket socket = createSocket(timeout);
		changeSocket(socket);
		setEncoding(Encoding.getJVMEncoding("UTF-8"));

		int2Buf = new byte[2];
		int4Buf = new byte[4];
	}

	public TrainDBStream(TrainDBStream stream, int timeout) throws IOException {
		/*
		 * Some defaults
		 */
		int sendBufferSize = 1024;
		int receiveBufferSize = 1024;
		int soTimeout = 0;
		boolean keepAlive = false;

		/*
		 * Get the existing values before closing the stream
		 */
		try {
			sendBufferSize = stream.getSocket().getSendBufferSize();
			receiveBufferSize = stream.getSocket().getReceiveBufferSize();
			soTimeout = stream.getSocket().getSoTimeout();
			keepAlive = stream.getSocket().getKeepAlive();
		} catch (SocketException ex) {
			// ignore it
		}
		// close the existing stream
		stream.close();

		this.socketFactory = stream.socketFactory;
		this.hostSpec = stream.hostSpec;

		Socket socket = createSocket(timeout);
		changeSocket(socket);
		setEncoding(Encoding.getJVMEncoding("UTF-8"));
		// set the buffer sizes and timeout
		socket.setReceiveBufferSize(receiveBufferSize);
		socket.setSendBufferSize(sendBufferSize);
		setNetworkTimeout(soTimeout);
		socket.setKeepAlive(keepAlive);

		int2Buf = new byte[2];
		int4Buf = new byte[4];
	}

	public HostSpec getHostSpec() {
		return hostSpec;
	}

	public Socket getSocket() {
		return connection;
	}

	public SocketFactory getSocketFactory() {
		return socketFactory;
	}

	private Socket createSocket(int timeout) throws IOException {
		Socket socket = socketFactory.createSocket();
		String localSocketAddress = hostSpec.getLocalSocketAddress();

		if (localSocketAddress != null) {
			socket.bind(new InetSocketAddress(InetAddress.getByName(localSocketAddress), 0));
		}

		if (!socket.isConnected()) {
			// When using a SOCKS proxy, the host might not be resolvable locally,
			// thus we defer resolution until the traffic reaches the proxy. If there
			// is no proxy, we must resolve the host to an IP to connect the socket.
			InetSocketAddress address = hostSpec.shouldResolve()
					? new InetSocketAddress(hostSpec.getHost(), hostSpec.getPort())
					: InetSocketAddress.createUnresolved(hostSpec.getHost(), hostSpec.getPort());

			socket.connect(address, timeout);
		}

		return socket;
	}

	public void changeSocket(Socket socket) throws IOException {
		assert connection != socket : "changeSocket is called with the current socket as argument."
				+ " This is a no-op, however, it re-allocates buffered streams, so refrain from"
				+ " excessive changeSocket calls";

		this.connection = socket;

		// Submitted by Jason Venner <jason@idiom.com>. Disable Nagle
		// as we are selective about flushing output only when we
		// really need to.
		connection.setTcpNoDelay(true);

		// Buffer sizes submitted by Sverre H Huseby <sverrehu@online.no>
		input = new VisibleBufferedInputStream(connection.getInputStream(), 8192);
		output = new BufferedOutputStream(connection.getOutputStream(), 8192);

		if (encoding != null) {
			setEncoding(encoding);
		}
	}

	public Encoding getEncoding() {
		return encoding;
	}

	/**
	 * Change the encoding used by this connection.
	 *
	 * @param encoding the new encoding to use
	 * @throws IOException if something goes wrong
	 */
	public void setEncoding(Encoding encoding) throws IOException {
		if (this.encoding != null && this.encoding.name().equals(encoding.name())) {
			return;
		}
		// Close down any old writer.
		if (encodingWriter != null) {
			encodingWriter.close();
		}

		this.encoding = encoding;

		// Intercept flush() downcalls from the writer; our caller
		// will call stream.flush() as needed.
		OutputStream interceptor = new FilterOutputStream(output) {
			public void flush() throws IOException {
			}

			public void close() throws IOException {
				super.flush();
			}
		};

		encodingWriter = encoding.getEncodingWriter(interceptor);
	}

	public Writer getEncodingWriter() throws IOException {
		if (encodingWriter == null) {
			throw new IOException("No encoding has been set on this connection");
		}

		return encodingWriter;
	}
	
	public void sendByte(byte val) throws IOException {
		output.write(val);
	}
	
	/**
	 * Sends a single character to the back end.
	 *
	 * @param val the character to be sent
	 * @throws IOException if an I/O error occurs
	 */
	public void sendChar(int val) throws IOException {
		output.write(val);
	}

	/**
	 * Sends a 4-byte integer to the back end.
	 *
	 * @param val the integer to be sent
	 * @throws IOException if an I/O error occurs
	 */
	public void sendInteger4(int val) throws IOException {
		int4Buf[0] = (byte) (val >>> 24);
		int4Buf[1] = (byte) (val >>> 16);
		int4Buf[2] = (byte) (val >>> 8);
		int4Buf[3] = (byte) (val);
		
		output.write(int4Buf);
	}

	public void send(byte[] buf) throws IOException {
		output.write(buf);
	}

	public void send(byte[] buf, int siz) throws IOException {
		send(buf, 0, siz);
	}

	public void send(byte[] buf, int off, int siz) throws IOException {
		int bufamt = buf.length - off;
		output.write(buf, off, bufamt < siz ? bufamt : siz);
		for (int i = bufamt; i < siz; ++i) {
			output.write(0);
		}
	}

	/**
	 * Receives a single character from the backend.
	 * @return the character received
	 * @throws IOException if an I/O Error occurs
	 */
	public int receiveChar() throws IOException {
		int c = input.read();
		if (c < 0) {
			throw new EOFException();
		}
		return c;
	}

	/**
	 * Receives a four byte integer from the backend.
	 * @return the integer received from the backend
	 * @throws IOException if an I/O error occurs
	 */
	public int receiveInteger4() throws IOException {
		if (input.read(int4Buf) != 4) {
			throw new EOFException();
		}

		return (int4Buf[0] & 0xFF) << 24 | (int4Buf[1] & 0xFF) << 16 | (int4Buf[2] & 0xFF) << 8 | int4Buf[3] & 0xFF;
	}

	/**
	 * Receives a two byte integer from the backend.
	 * @return the integer received from the backend 
	 * @throws IOException if an I/O error occurs
	 */
	public int receiveInteger2() throws IOException {
		if (input.read(int2Buf) != 2) {
			throw new EOFException();
		}

		return (int2Buf[0] & 0xFF) << 8 | int2Buf[1] & 0xFF;
	}

	/**
	 * Receives a null-terminated string from the backend and attempts to decode to a
	 * {@link Encoding#decodeCanonicalized(byte[], int, int) canonical} {@code String}.
	 * If we don't see a null, then we assume something has gone wrong.
	 *
	 * @return string from back end
	 * @throws IOException if an I/O error occurs, or end of file
	 * @see Encoding#decodeCanonicalized(byte[], int, int)
	 */
	public String receiveCanonicalString() throws IOException {
		int len = input.scanCStringLength();
		String res = encoding.decodeCanonicalized(input.getBuffer(), input.getIndex(), len - 1);
		input.skip(len);
		return res;
	}
	
	/**
	 * Read a tuple from the back end. A tuple is a two dimensional array of bytes. This variant reads
	 * the protocol's tuple representation.
	 * @return tuple from the back end
	 * @throws IOException if a data I/O error occurs
	 * @throws OutOfMemoryError
	 * @throws SQLException if read more bytes than set maxResultBuffer
	 */
	public Tuple receiveTuple() throws IOException, OutOfMemoryError, SQLException {
	    int messageSize = receiveInteger4(); // MESSAGE SIZE
	    int nf = receiveInteger2();
		//size = messageSize - 4 bytes of message size - 2 bytes of field count - 4 bytes for each column length
		int dataToReadSize = messageSize - 4 - 2 - 4 * nf;
		setMaxRowSizeBytes(dataToReadSize);
	    
	    byte[][] answer = new byte[nf][];

		increaseByteCounter(dataToReadSize);
		OutOfMemoryError oom = null;
	    
	    for (int i = 0; i < nf; ++i) {
			int size = receiveInteger4();
			if (size != -1) {
				try {
					answer[i] = new byte[size];
					receive(answer[i], 0, size);
				} catch (OutOfMemoryError oome) {
					oom = oome;
					skip(size);
				}
			}
		}
	    
	    if (oom != null) {
	    	throw oom;
	    }
	    
	    return new Tuple(answer);
	}


	public String receiveString(int len) throws IOException {
		if (!input.ensureBytes(len)) {
			throw new EOFException();
		}

		String res = encoding.decode(input.getBuffer(), input.getIndex(), len);
		input.skip(len);
		return res;
	}

	public EncodingPredictor.DecodeResult receiveErrorString(int len) throws IOException {
		if (!input.ensureBytes(len)) {
			throw new EOFException();
		}

		EncodingPredictor.DecodeResult res;
		try {
			String value = encoding.decode(input.getBuffer(), input.getIndex(), len);
			// no autodetect warning as the message was converted on its own
			res = new EncodingPredictor.DecodeResult(value, null);
		} catch (IOException e) {
			res = EncodingPredictor.decode(input.getBuffer(), input.getIndex(), len);
			if (res == null) {
				Encoding enc = Encoding.defaultEncoding();
				String value = enc.decode(input.getBuffer(), input.getIndex(), len);
				res = new EncodingPredictor.DecodeResult(value, enc.name());
			}
		}
		input.skip(len);
		return res;
	}

	public String receiveString() throws IOException {
		int len = input.scanCStringLength();
		String res = encoding.decode(input.getBuffer(), input.getIndex(), len - 1);
		input.skip(len);
		return res;
	}

	public byte[] receive(int siz) throws IOException {
		byte[] answer = new byte[siz];
		receive(answer, 0, siz);
		return answer;
	}

	/**
	 * Reads in a given number of bytes from the backend.
	 *
	 * @param buf buffer to store result
	 * @param off offset in buffer
	 * @param siz number of bytes to read
	 * @throws IOException if a data I/O error occurs
	 */
	public void receive(byte[] buf, int off, int siz) throws IOException {
		int s = 0;

		while (s < siz) {
			int w = input.read(buf, off + s, siz - s);
			if (w < 0) {
				throw new EOFException();
			}
			s += w;
		}
	}

	public void skip(int size) throws IOException {
		long s = 0;
		while (s < size) {
			s += input.skip(size - s);
		}
	}

	public void sendStream(InputStream inStream, int remaining) throws IOException,
			TrainDBJdbcException {
		int expectedLength = remaining;
		if (streamBuffer == null) {
			streamBuffer = new byte[8192];
		}

		while (remaining > 0) {
			int count = (remaining > streamBuffer.length ? streamBuffer.length : remaining);
			int readCount;

			try {
				readCount = inStream.read(streamBuffer, 0, count);
				if (readCount < 0) {
					throw new TrainDBJdbcException(
							MessageFormat.format("Premature end of input stream, expected {0} bytes, but only read {1}.",
									expectedLength, expectedLength - remaining));
				}
			} catch (IOException ioe) {
				while (remaining > 0) {
					send(streamBuffer, count);
					remaining -= count;
					count = (remaining > streamBuffer.length ? streamBuffer.length : remaining);
				}

				throw new TrainDBBindException(ioe);
			}

			send(streamBuffer, readCount);
			remaining -= readCount;
		}
	}

	public void setNetworkTimeout(int milliseconds) throws IOException {
		connection.setSoTimeout(milliseconds);
		input.setTimeoutRequested(milliseconds != 0);
	}

	public int getNetworkTimeout() throws IOException {
		return connection.getSoTimeout();
	}

	public boolean isClosed() {
		return connection.isClosed();
	}
	
	public void setMaxResultBuffer(@Nullable String value) throws TrainDBJdbcException {
		// maxResultBuffer = PGPropertyMaxResultBufferParser.parseProperty(value);
	}
	
	public void setMaxRowSizeBytes(int rowSizeBytes) {
		if (rowSizeBytes > maxRowSizeBytes) {
			maxRowSizeBytes = rowSizeBytes;
		}
	}
	
	private void increaseByteCounter(long value) throws SQLException {
		if (maxResultBuffer != -1) {
			resultBufferByteCount += value;
			if (resultBufferByteCount > maxResultBuffer) {
				throw new TrainDBJdbcException(MessageFormat.format("Result set exceeded maxResultBuffer limit. Received:  {0}; Current limit: {1}", String.valueOf(resultBufferByteCount), String.valueOf(maxResultBuffer)),TrainDBState.COMMUNICATION_ERROR);
			}
		}
	}

	@Override
	public void flush() throws IOException {
		if (encodingWriter != null) {
			encodingWriter.flush();
		}

		output.flush();
	}

	@Override
	public void close() throws IOException {
		if (encodingWriter != null) {
			encodingWriter.close();
		}

		output.close();
		input.close();
		connection.close();
	}
}
