package traindb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import traindb.jdbc.util.GT;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBLogger;
import traindb.jdbc.util.TrainDBProperty;
import traindb.jdbc.util.TrainDBState;

public final class Driver implements java.sql.Driver {
	private static final TrainDBLogger logger = new TrainDBLogger();

	public static final int DEBUG = 2;
	public static final int INFO = 1;
	public static final int OFF = 0;

	public static final int MAJORVERSION = 0;
	public static final int MINORVERSION = 1;

	private static final String DEFAULT_TRAINDB_PORT = "58000";

	private static final String[] PROTOCOLS = { "jdbc", "traindb" };
	private static final String TRAINDB_PROTOCOL = String.format("%s:%s:", (Object[]) PROTOCOLS);

	private static Driver registeredDriver;
	private Properties defaultProperties;
	private static boolean logLevelSet;

	static {
		try {
			// Driver.setLogLevel(DEBUG);
			
			// moved the registerDriver from the constructor to here
			// because some clients call the driver themselves (I know, as
			// my early jdbc work did - and that was based on other examples).
			// Placing it here, means that the driver is registered once only.
			register();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public static void register() throws SQLException {
		if (isRegistered())
			throw new IllegalStateException("Driver is already registered. It can only be registered once.");

		Driver driver = new Driver();
		DriverManager.registerDriver(driver);
		Driver.registeredDriver = driver;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		logger.debug("Call Connect : " + url);
		
		Properties defaults;

		if (!url.startsWith(TRAINDB_PROTOCOL))
			return null;

		try {
			defaults = getDefaultProperties();
		} catch (IOException ioe) {
			throw new TrainDBJdbcException(GT.tr("Error loading default settings from driverconfig.properties"), TrainDBState.UNEXPECTED_ERROR, ioe);
		}

		// override defaults with provided properties
		Properties props = new Properties(defaults);
		
		if (info != null) {
			Enumeration<?> e = info.propertyNames();
			while (e.hasMoreElements()) {
				String propName = (String) e.nextElement();
				String propValue = info.getProperty(propName);
				if (propValue == null) {
					throw new TrainDBJdbcException(GT.tr("Properties for the driver contains a non-string value for the key ") + propName, TrainDBState.UNEXPECTED_ERROR);
				}
				
				props.setProperty(propName, propValue);
			}
		}
		
		// parse URL and add more properties
		/*
		props = parseURL(url, props);
		if (props == null) {
			logger.debug("Error in url: " + url);
			return null;
		}
		*/
		
		try {
			logger.debug("Connecting with URL: " + url);

			long timeout = timeout(props);
			
			if (timeout <= 0)
				return makeConnection(url, props);

			ConnectThread ct = new ConnectThread(url, props);
			Thread thread = new Thread(ct, "TrainDB JDBC driver connection thread");
			thread.setDaemon(true); // Don't prevent the VM from shutting down
			thread.start();
			return ct.getResult(timeout);
		} catch (TrainDBJdbcException ex1) {
			logger.debug("Connection error:", ex1);
			throw ex1;
		} catch (AccessControlException ace) {
			throw new TrainDBJdbcException(GT.tr("Your security policy has prevented the connection from being attempted.  You probably need to grant the connect java.net.SocketPermission to the database server host and port that you wish to connect to."), TrainDBState.UNEXPECTED_ERROR, ace);
		} catch (Exception ex2) {
			logger.debug("Unexpected connection error:", ex2);
			throw new TrainDBJdbcException(GT.tr("Something unusual has occurred to cause the driver to fail. Please report this exception."), TrainDBState.UNEXPECTED_ERROR, ex2);
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return parseURL(url, null) != null;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMajorVersion() {
		return MAJORVERSION;
	}

	@Override
	public int getMinorVersion() {
		return MINORVERSION;
	}

	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw notImplemented(this.getClass(), "getParentLogger()");
	}

	public static boolean isRegistered() {
		return registeredDriver != null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private synchronized Properties getDefaultProperties() throws IOException {
		if (defaultProperties != null)
			return defaultProperties;

		// Make sure we load properties with the maximum possible
		// privileges.
		try {
			defaultProperties = (Properties) AccessController.doPrivileged(new PrivilegedExceptionAction() {
				public Object run() throws IOException {
					return loadDefaultProperties();
				}
			});
		} catch (PrivilegedActionException e) {
			throw (IOException) e.getException();
		}

		// Use the loglevel from the default properties (if any)
		// as the driver-wide default unless someone explicitly called
		// setLogLevel() already.
		synchronized (Driver.class) {
			if (!logLevelSet) {
				String driverLogLevel = TrainDBProperty.LOG_LEVEL.get(defaultProperties);
				if (driverLogLevel != null) {
					try {
						setLogLevel(Integer.parseInt(driverLogLevel));
					} catch (Exception ignore) {
						// invalid value for loglevel; ignore it
					}
				}
			}
		}

		return defaultProperties;
	}

	private Properties loadDefaultProperties() throws IOException {
		Properties merged = new Properties();

		try {
			TrainDBProperty.USER.set(merged, System.getProperty("user.name"));
		} catch (java.lang.SecurityException se) {
			// We're just trying to set a default, so if we can't
			// it's not a big deal.
		}

		// If we are loaded by the bootstrap classloader, getClassLoader()
		// may return null. In that case, try to fall back to the system
		// classloader.
		//
		// We should not need to catch SecurityException here as we are
		// accessing either our own classloader, or the system classloader
		// when our classloader is null. The ClassLoader javadoc claims
		// neither case can throw SecurityException.
		ClassLoader cl = getClass().getClassLoader();
		if (cl == null)
			cl = ClassLoader.getSystemClassLoader();

		if (cl == null) {
			logger.debug("Can't find a classloader for the Driver; not loading driver configuration");
			return merged; // Give up on finding defaults.
		}

		logger.debug("Loading driver configuration via classloader " + cl);

		// When loading the driver config files we don't want settings found
		// in later files in the classpath to override settings specified in
		// earlier files. To do this we've got to read the returned
		// Enumeration into temporary storage.
		ArrayList<URL> urls = new ArrayList<URL>();
		Enumeration<URL> urlEnum = cl.getResources("conf/driverconfig.properties");
		
		while (urlEnum.hasMoreElements())
			urls.add(urlEnum.nextElement());
		
		for (int i = urls.size() - 1; i >= 0; i--) {
			URL url = urls.get(i);
			
			logger.debug("Loading driver configuration from: " + url);
			InputStream is = url.openStream();
			merged.load(is);
			is.close();
		}

		return merged;
	}

	public static void setLogLevel(int logLevel) {
		synchronized (Driver.class) {
			logger.setLogLevel(logLevel);
			logLevelSet = true;
		}
	}

	public static int getLogLevel() {
		synchronized (Driver.class) {
			return logger.getLogLevel();
		}
	}

	public static SQLFeatureNotSupportedException notImplemented(Class callClass, String functionName) {
		return new SQLFeatureNotSupportedException(GT.tr("Method {0} is not yet implemented.", callClass.getName() + "." + functionName), TrainDBState.NOT_IMPLEMENTED.getState());
	}

	public static Properties parseURL(String url, Properties defaults) {
		Properties urlProps = new Properties(defaults);

		String urlServer = url;
		String urlArgs = "";

		int qPos = url.indexOf('?');
		if (qPos != -1) {
			urlServer = url.substring(0, qPos);
			urlArgs = url.substring(qPos + 1);
		}

		if (!urlServer.startsWith(TRAINDB_PROTOCOL))
			return null;

		urlServer = urlServer.substring(TRAINDB_PROTOCOL.length());

		if (urlServer.startsWith("//")) { // FIXME: Environment variables
			urlServer = urlServer.substring(2);
			int slashIdx = urlServer.indexOf('/');
			if (slashIdx > -1)
				urlServer = urlServer.substring(0, slashIdx);

			StringBuilder hosts = new StringBuilder();
			StringBuilder ports = new StringBuilder();
			String[] addrs = urlServer.split(",");
			for (String addr : addrs) {
				addr = addr.trim();

				String host;
				String port;

				int portIdx = addr.lastIndexOf(':');
				if (portIdx > -1 && addr.lastIndexOf(']') < portIdx) {
					host = addr.substring(0, portIdx);
					port = addr.substring(portIdx + 1);
					if (port.isEmpty()) {
						port = DEFAULT_TRAINDB_PORT;
					} else {
						try {
							Integer.parseInt(port);
						} catch (NumberFormatException e) {
							return null;
						}
					}
				} else {
					host = addr;
					port = DEFAULT_TRAINDB_PORT;
				}

				if (host.isEmpty())
					host = "localhost";

				hosts.append(host).append(',');
				ports.append(port).append(',');
			}

			hosts.setLength(hosts.length() - 1);
			ports.setLength(hosts.length() - 1);

			urlProps.setProperty("server.host", hosts.toString());
			urlProps.setProperty("server.port", ports.toString());
			/* urlProps.setProperty("TrainDBNAME", "<unknown>"); */
		} else {
			urlProps.setProperty("server.host", "localhost");
			urlProps.setProperty("server.port", DEFAULT_TRAINDB_PORT);
			/* urlProps.setProperty("TrainDBNAME", "<unknown>"); */
		}

		// parse the args part of the url
		String[] args = urlArgs.split("&");
		for (String token : args) {
			if (token.isEmpty())
				continue;

			int pos = token.indexOf('=');
			if (pos > -1)
				urlProps.setProperty(token.substring(0, pos), token.substring(pos + 1));
			else
				urlProps.setProperty(token, "");
		}

		return urlProps;
	}

	protected static long timeout(Properties props) {
		final int msecPerSec = 1000;

		String timeout = TrainDBProperty.LOGIN_TIMEOUT.get(props);

		if (timeout != null) {
			try {
				return (long) (Float.parseFloat(timeout) * msecPerSec);
			} catch (NumberFormatException e) {
				// Log level isn't set yet, so this doesn't actually
				// get printed.
				logger.debug("Couldn't parse loginTimeout value: " + timeout);
			}
		}

		return (long) DriverManager.getLoginTimeout() * msecPerSec;
	}

	/**
	 * Perform a connect in a separate thread; supports getting the results from the
	 * original thread while enforcing a login timeout.
	 */
	private static class ConnectThread implements Runnable {
		private final String url;
		private final Properties props;
		private Connection result;
		private Throwable resultException;
		private boolean abandoned;

		ConnectThread(String url, Properties props) {
			this.url = url;
			this.props = props;
		}

		public void run() {
			Connection conn;
			Throwable error;

			try {
				conn = makeConnection(url, props);
				error = null;
			} catch (Throwable t) {
				conn = null;
				error = t;
			}

			synchronized (this) {
				if (abandoned) {
					if (conn != null) {
						try {
							conn.close();
						} catch (SQLException ignored) {}
					}
				} else {
					result = conn;
					resultException = error;
					notify();
				}
			}
		}

		/**
		 * Get the connection result from this (assumed running) thread. If the timeout
		 * is reached without a result being available, a SQLException is thrown.
		 *
		 * @param timeout timeout in milliseconds
		 * @return the new connection, if successful
		 * @throws SQLException if a connection error occurs or the timeout is reached
		 */
		public Connection getResult(long timeout) throws SQLException {
			long expiry = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + timeout;
			synchronized (this) {
				while (true) {
					if (result != null)
						return result;

					if (resultException != null) {
						if (resultException instanceof SQLException) {
							resultException.fillInStackTrace();
							throw (SQLException) resultException;
						} else {
							throw new TrainDBJdbcException(GT.tr("Something unusual has occurred to cause the driver to fail. Please report this exception."), TrainDBState.UNEXPECTED_ERROR, resultException);
						}
					}

					long delay = expiry - TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
					if (delay <= 0) {
						abandoned = true;
						throw new TrainDBJdbcException(GT.tr("Connection attempt timed out."), TrainDBState.CONNECTION_UNABLE_TO_CONNECT);
					}

					try {
						wait(delay);
					} catch (InterruptedException ie) {
						// reset the interrupt flag
						Thread.currentThread().interrupt();
						abandoned = true;

						// throw an unchecked exception which will hopefully not be ignored by the
						// calling code
						throw new RuntimeException(GT.tr("Interrupted while attempting to connect."));
					}
				}
			}
		}
	}

	private static Connection makeConnection(String url, Properties props) throws SQLException {
		return new TrainDBConnection(url, props);
	}

	/**
	 * @return the username of the URL
	 */
	protected static String user(Properties props) {
		return props.getProperty("user", "");
	}
}
