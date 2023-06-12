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

import com.google.gson.Gson;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.SocketFactory;
import traindb.jdbc.util.HostSpec;
import traindb.jdbc.util.TrainDBProperty;

/**
 * ConnectionFactory implementation for connections.
 */
public class ConnectionFactoryImpl extends ConnectionFactory {
  private static final Logger LOGGER = Logger.getLogger(ConnectionFactoryImpl.class.getName());

  @Override
  public QueryExecutor openConnectionImpl(String url, Properties info) throws SQLException {
    SocketFactory socketFactory = SocketFactory.getDefault();// .getSocketFactory(info);

    TrainDBStream newStream = null;

    try {
      newStream = tryConnect(url, info, socketFactory);
    } catch (SQLException e) {

    } catch (IOException e) {
      e.printStackTrace();
    }

    QueryExecutor queryExecutor = new QueryExecutor(newStream, info);

    // runInitialQueries(queryExecutor, info);

    return queryExecutor;
  }

  private TrainDBStream tryConnect(String url, Properties info, SocketFactory socketFactory)
      throws SQLException, IOException {
    int connectTimeout = TrainDBProperty.CONNECT_TIMEOUT.getInt(info) * 1000;

    HostSpec hostSpec = new HostSpec(info.getProperty("server.host"),
        Integer.parseInt(info.getProperty("server.port")));

    TrainDBStream newStream = new TrainDBStream(socketFactory, hostSpec, connectTimeout);

    // Set the socket timeout if the "socketTimeout" property has been set.
    int socketTimeout = TrainDBProperty.SOCKET_TIMEOUT.getInt(info);
    if (socketTimeout > 0) {
      newStream.setNetworkTimeout(socketTimeout * 1000);
    }

    // Enable TCP keep-alive probe if required.
    boolean requireTCPKeepAlive = TrainDBProperty.TCP_KEEP_ALIVE.getBoolean(info);
    newStream.getSocket().setKeepAlive(requireTCPKeepAlive);

    // Try to set SO_SNDBUF and SO_RECVBUF socket options, if requested.
    // If receiveBufferSize and send_buffer_size are set to a value greater
    // than 0, adjust. -1 means use the system default, 0 is ignored since not
    // supported.

    // Set SO_RECVBUF read buffer size
    int receiveBufferSize = TrainDBProperty.RECEIVE_BUFFER_SIZE.getInt(info);
    if (receiveBufferSize > -1) {
      // value of 0 not a valid buffer size value
      if (receiveBufferSize > 0) {
        newStream.getSocket().setReceiveBufferSize(receiveBufferSize);
      } else {
        LOGGER.log(Level.WARNING, "Ignore invalid value for receiveBufferSize: {0}",
            receiveBufferSize);
      }
    }

    // Set SO_SNDBUF write buffer size
    int sendBufferSize = TrainDBProperty.SEND_BUFFER_SIZE.getInt(info);
    if (sendBufferSize > -1) {
      if (sendBufferSize > 0) {
        newStream.getSocket().setSendBufferSize(sendBufferSize);
      } else {
        LOGGER.log(Level.WARNING, "Ignore invalid value for sendBufferSize: {0}", sendBufferSize);
      }
    }

    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.log(Level.FINE, "Receive Buffer Size is {0}",
          newStream.getSocket().getReceiveBufferSize());
      LOGGER.log(Level.FINE, "Send Buffer Size is {0}", newStream.getSocket().getSendBufferSize());
    }

		/*
		newStream = enableGSSEncrypted(newStream, gssEncMode, hostSpec.getHost(), user, info, connectTimeout);

		// if we have a security context then gss negotiation succeeded. Do not attempt
		// SSL negotiation
		if (!newStream.isGssEncrypted()) {
			// Construct and send an ssl startup packet if requested.
			newStream = enableSSL(newStream, sslMode, info, connectTimeout);
		}
		*/

    // Make sure to set network timeout again, in case the stream changed due to GSS
    // or SSL
    if (socketTimeout > 0) {
      newStream.setNetworkTimeout(socketTimeout * 1000);
    }

		/*
		List<String[]> paramList = getParametersForStartup(user, database, info);
		sendStartupPacket(newStream, paramList);
		*/

    List<String[]> paramList = new ArrayList<String[]>();
    paramList.add(new String[] {"url", url});
    paramList.add(new String[] {"user", info.getProperty("user")});
    paramList.add(new String[] {"password", info.getProperty("password", "")});

    sendStartupPacket(newStream, paramList);

    return newStream;
  }

  private void sendStartupPacket(TrainDBStream stream, List<String[]> params) throws IOException {
    Properties pros = new Properties();

    for (int i = 0; i < params.size(); ++i) {
      pros.setProperty(params.get(i)[0], params.get(i)[1]);
    }

    byte[] data = new Gson().toJson(pros).getBytes();

    stream.sendChar('S');
    stream.sendInteger4(4 + data.length);
    stream.send(data);
    stream.flush();
  }
}