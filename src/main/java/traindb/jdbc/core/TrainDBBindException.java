package traindb.jdbc.core;

import java.io.IOException;

@SuppressWarnings("serial")
public class TrainDBBindException extends IOException {

	private final IOException ioe;

	public TrainDBBindException(IOException ioe) {
	    this.ioe = ioe;
	  }

	public IOException getIOException() {
		return ioe;
	}
}