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

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Poor man's logging infrastructure. This just deals with maintaining a per-
 * connection ID and log level, and timestamping output.
 */
public final class TrainDBJdbcLogger {
  public static final int DEBUG = 2;
  public static final int INFO = 1;

  // For brevity we only log the time, not date or timezone (the main reason
  // for the timestamp is to see delays etc. between log lines, not to pin
  // down an instant in time)
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS ");
  private final FieldPosition dummyPosition = new FieldPosition(0);
  private final StringBuffer buffer = new StringBuffer();
  private final String connectionIDString;

  private int level = 0;

  public TrainDBJdbcLogger() {
    connectionIDString = "(driver) ";
  }

  public TrainDBJdbcLogger(int connectionID) {
    connectionIDString = "(" + connectionID + ") ";
  }

  public int getLogLevel() {
    return level;
  }

  public void setLogLevel(int level) {
    this.level = level;
  }

  public boolean logDebug() {
    return level >= DEBUG;
  }

  public boolean logInfo() {
    return level >= INFO;
  }

  public void debug(String str) {
    debug(str, null);
  }

  public void debug(String str, Throwable t) {
    if (logDebug()) {
      log(str, t);
    }
  }

  public void info(String str) {
    info(str, null);
  }

  public void info(String str, Throwable t) {
    if (logInfo()) {
      log(str, t);
    }
  }

  public void log(String str, Throwable t) {
    PrintWriter writer = DriverManager.getLogWriter();
    if (writer == null) {
      return;
    }

    synchronized (this) {
      buffer.setLength(0);
      dateFormat.format(new Date(), buffer, dummyPosition);
      buffer.append(connectionIDString);
      buffer.append(str);

      // synchronize to ensure that the exception (if any) does
      // not get split up from the corresponding log message
      synchronized (writer) {
        writer.println(buffer.toString());
        if (t != null) {
          t.printStackTrace(writer);
        }
      }
    }
  }
}

