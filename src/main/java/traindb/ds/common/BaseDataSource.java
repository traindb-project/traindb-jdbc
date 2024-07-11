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

import static traindb.jdbc.util.Nullness.castNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.CommonDataSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.jdbc.util.TrainDBJdbcException;
import traindb.jdbc.util.TrainDBState;

public abstract class BaseDataSource implements CommonDataSource, Referenceable {
  private static final Logger LOGGER = Logger.getLogger(BaseDataSource.class.getName());

  // Standard properties, defined in the JDBC 2.0 Optional Package spec
  private String[] serverNames = new String[] {"localhost"};
  private @Nullable String databaseName = "";
  private @Nullable String user;
  private @Nullable String password;
  private int[] portNumbers = new int[] {0};

  private Properties properties = new Properties();

  private final String TRAINDB_JDBC_CONFIG_FILENAME = "traindb-jdbc.properties";

  /*
   * Ensure the driver is loaded as JDBC Driver might be invisible to Java's ServiceLoader.
   */
  static {
    try {
      Class.forName("traindb.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "BaseDataSource is unable to load traindb.jdbc.Driver."
              + "Please check if you have proper TrainDB JDBC Driver jar on the classpath",
          e);
    }
  }

  public Connection getConnection() throws SQLException {
    return getConnection(user, password);
  }

  /**
   * Gets a connection to the database.
   * The database is identified by the properties serverName, databaseName, and portNumber.
   * The user to connect as is identified by the arguments user and password,
   * which override the DataSource properties by the same name.
   */
  public Connection getConnection(@Nullable String user, @Nullable String password)
      throws SQLException {
    try {
      loadConfiguration();
      Connection con = DriverManager.getConnection(getUrl(), user, password);
      this.user = user;
      this.password = password;
      if (LOGGER.isLoggable(Level.FINE)) {
        LOGGER.log(Level.FINE, "Created a {0} for {1} at {2}",
            new Object[] {getDescription(), user, getUrl()});
      }
      return con;
    } catch (SQLException e) {
      LOGGER.log(Level.FINE, "Failed to create a {0} for {1} at {2}: {3}",
          new Object[] {getDescription(), user, getUrl(), e});
      throw e;
    }
  }

  @Override
  public @Nullable PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(@Nullable PrintWriter printWriter) {
    // NOOP
  }

  @Override
  public int getLoginTimeout() {
    return Integer.parseInt(properties.getProperty("login.timeout", "0"));
  }

  @Override
  public void setLoginTimeout(int loginTimeout) {
    try {
      setProperty("login.timeout", Integer.toString(loginTimeout));
    } catch (Exception e) {
    }
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    return Logger.getLogger("traindb");
  }

  /**
   * Gets the name of the host the database is running on.
   */
  @Deprecated
  public String getServerName() {
    return serverNames[0];
  }

  /**
   * Gets the name of the host(s) the database is running on.
   */
  public String[] getServerNames() {
    return serverNames;
  }

  /**
   * Sets the name of the host the database is running on.
   * If this is changed, it will only affect future calls to getConnection.
   * The default value is {@code localhost}.
   */
  @Deprecated
  public void setServerName(String serverName) {
    this.setServerNames(new String[] {serverName});
  }

  /**
   * Sets the name of the host(s) the database is running on.
   * If this is changed, it will only affect future calls to getConnection.
   * The default value is {@code localhost}.
   */
  @SuppressWarnings("nullness")
  public void setServerNames(@Nullable String @Nullable [] serverNames) {
    if (serverNames == null || serverNames.length == 0) {
      this.serverNames = new String[] {"localhost"};
    } else {
      serverNames = serverNames.clone();
      for (int i = 0; i < serverNames.length; i++) {
        String serverName = serverNames[i];
        if (serverName == null || serverName.equals("")) {
          serverNames[i] = "localhost";
        }
      }
      this.serverNames = serverNames;
    }
  }

  /**
   * Gets the name of the database, running on the server identified by the serverName property.
   */
  public @Nullable String getDatabaseName() {
    return databaseName;
  }

  /**
   * Sets the name of the database, running on the server identified by the serverName property.
   * If this is changed, it will only affect future calls to getConnection.
   */
  public void setDatabaseName(@Nullable String databaseName) {
    this.databaseName = databaseName;
  }

  public abstract String getDescription();

  /**
   * Gets the user to connect as by default.
   */
  public @Nullable String getUser() {
    return user;
  }

  /**
   * Sets the user to connect as by default.
   * If this is not specified, you must use the getConnection method which takes
   * a user and password as parameters.
   * If this is changed, it will only affect future calls to getConnection.
   */
  public void setUser(@Nullable String user) {
    this.user = user;
  }

  /**
   * Gets the password to connect with by default.
   */
  public @Nullable String getPassword() {
    return password;
  }

  /**
   * Sets the password to connect with by default.
   * If this is not specified but a password is needed to log in,
   * you must use the getConnection method which takes a user and password as parameters.
   * If this is changed, it will only affect future calls to getConnection.
   */
  public void setPassword(@Nullable String password) {
    this.password = password;
  }

  /**
   * Gets the port which the server is listening on for TCP/IP connections.
   */
  @Deprecated
  public int getPortNumber() {
    if (portNumbers == null || portNumbers.length == 0) {
      return 0;
    }
    return portNumbers[0];
  }

  /**
   * Gets the port(s) which the server is listening on for TCP/IP connections.
   */
  public int[] getPortNumbers() {
    return portNumbers;
  }

  /**
   * Sets the port which the server is listening on for TCP/IP connections.
   * If this is not set, or set to 0, the default port will be used.
   */
  @Deprecated
  public void setPortNumber(int portNumber) {
    setPortNumbers(new int[] {portNumber});
  }

  /**
   * Sets the port(s) which the server is listening on for TCP/IP connections.
   * If this is not set, or set to 0, the default port will be used.
   *
   * @param portNumbers port(s) which the PostgreSQL server is listening on for TCP/IP
   */
  public void setPortNumbers(int @Nullable [] portNumbers) {
    if (portNumbers == null || portNumbers.length == 0) {
      portNumbers = new int[] {0};
    }
    this.portNumbers = Arrays.copyOf(portNumbers, portNumbers.length);
  }

  public void loadConfiguration() {
    try {
      loadConfigurationFile(properties, TRAINDB_JDBC_CONFIG_FILENAME);
    } catch (Exception e) {
      // ignore
    }
  }

  private void loadConfigurationFile(Properties props, String filename) throws Exception {
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(filename);
    props.load(inputStream);
  }

  /**
   * Generates a {@link DriverManager} URL from the other properties supplied.
   */
  public String getUrl() {
    String traindb_host = properties.getProperty("TRAINDB_SERVER_HOST", "localhost");
    String traindb_port = properties.getProperty("TRAINDB_SERVER_PORT", "58000");
    String traindb_url_protocol = properties.getProperty("TRAINDB_URL_PROTOCOL", "jdbc:traindb");

    StringBuilder url = new StringBuilder(1024);
    url.append(traindb_url_protocol).append("://");
    for (int i = 0; i < serverNames.length; i++) {
      if (i > 0) {
        url.append(",");
      }
      url.append(serverNames[i]);
      if (portNumbers != null && portNumbers.length >= i && portNumbers[i] != 0) {
        url.append(":").append(portNumbers[i]);
      }
    }
    url.append("/");
    if (databaseName != null) {
      url.append(databaseName);
    }

    url.append("?server.host=").append(traindb_host);
    url.append("&server.port=").append(traindb_port);

    return url.toString();
  }

  /**
   * Generates a {@link DriverManager} URL from the other properties supplied.
   */
  public String getURL() {
    return getUrl();
  }

  /**
   * Sets properties from a {@link DriverManager} URL.
   */
  public void setUrl(String url) {
    Properties p = traindb.jdbc.Driver.parseURL(url, null);
    if (p == null) {
      throw new IllegalArgumentException("URL invalid " + url);
    }
    this.properties = p;
  }

  /**
   * Sets properties from a {@link DriverManager} URL.
   * Added to follow convention used in other DBMS.
   */
  public void setURL(String url) {
    setUrl(url);
  }

  public @Nullable String getProperty(String name) throws SQLException {
    String v = (String) properties.get(name);
    if (v == null) {
      throw new TrainDBJdbcException(
          "Unsupported property name: {0}", TrainDBState.INVALID_PARAMETER_VALUE);
    }
    return v;
  }

  public void setProperty(String name, @Nullable String value) throws SQLException {
    properties.setProperty(name, value);
  }

  /**
   * Generates a reference using the appropriate object factory.
   */
  protected Reference createReference() {
    return new Reference(getClass().getName(), TrainDBObjectFactory.class.getName(), null);
  }

  public Reference getReference() throws NamingException {
    Reference ref = createReference();
    StringBuilder serverString = new StringBuilder();
    for (int i = 0; i < serverNames.length; i++) {
      if (i > 0) {
        serverString.append(",");
      }
      String serverName = serverNames[i];
      serverString.append(serverName);
    }
    ref.add(new StringRefAddr("serverName", serverString.toString()));

    StringBuilder portString = new StringBuilder();
    for (int i = 0; i < portNumbers.length; i++) {
      if (i > 0) {
        portString.append(",");
      }
      int p = portNumbers[i];
      portString.append(Integer.toString(p));
    }
    ref.add(new StringRefAddr("portNumber", portString.toString()));
    ref.add(new StringRefAddr("databaseName", databaseName));
    if (user != null) {
      ref.add(new StringRefAddr("user", user));
    }
    if (password != null) {
      ref.add(new StringRefAddr("password", password));
    }

    return ref;
  }

  public void setFromReference(Reference ref) {
    databaseName = getReferenceProperty(ref, "databaseName");
    String portNumberString = getReferenceProperty(ref, "portNumber");
    if (portNumberString != null) {
      String[] ps = portNumberString.split(",");
      int[] ports = new int[ps.length];
      for (int i = 0; i < ps.length; i++) {
        try {
          ports[i] = Integer.parseInt(ps[i]);
        } catch (NumberFormatException e) {
          ports[i] = 0;
        }
      }
      setPortNumbers(ports);
    } else {
      setPortNumbers(null);
    }
    String serverName = castNonNull(getReferenceProperty(ref, "serverName"));
    setServerNames(serverName.split(","));
  }

  private static @Nullable String getReferenceProperty(Reference ref, String propertyName) {
    RefAddr addr = ref.get(propertyName);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

  protected void writeBaseObject(ObjectOutputStream out) throws IOException {
    out.writeObject(serverNames);
    out.writeObject(databaseName);
    out.writeObject(user);
    out.writeObject(password);
    out.writeObject(portNumbers);

    out.writeObject(properties);
  }

  protected void readBaseObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    serverNames = (String[]) in.readObject();
    databaseName = (String) in.readObject();
    user = (String) in.readObject();
    password = (String) in.readObject();
    portNumbers = (int[]) in.readObject();

    properties = (Properties) in.readObject();
  }

  public void initializeFrom(BaseDataSource source) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    source.writeBaseObject(oos);
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    readBaseObject(ois);
  }

}
