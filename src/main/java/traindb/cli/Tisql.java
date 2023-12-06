package traindb.cli;

import java.sql.*;
import java.util.Scanner;

public class Tisql {
    private final String DRIVER = "traindb.jdbc.Driver";
    private Scanner scanner = null;
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private boolean isConnected = false;
    private boolean isMoreResult = false;

    public Tisql() {
        scanner = new Scanner(System.in);
	try { 
            Class.forName(DRIVER);
	} catch ( Exception e ) {
	    e.printStackTrace();
	}
    }

    public void printHelp() {
        System.out.println("Tisql Help");
        System.out.println("----------");
        System.out.println("!connect <driver> <host> <port> <database> <user> <password>");
        System.out.println("!connect <driver> <host> <port> <database>");
        System.out.println("!disconnect");
        System.out.println("!moreresult <on|off>");
        System.out.println("!exit");
        System.out.println("----------");
    }

    public void processCommands(String command) throws ClassNotFoundException, SQLException {
        // Process Commands
        String[] tokens = command.split(" ");
        if (tokens.length == 0) {
            System.out.println("Invalid command");
            return;
        }

        if (tokens[0].equals("exit")) {
            System.out.println("Exit command");
            System.exit(0);
        }

        switch (tokens[0]) {
            case "connect":
                String driver = null;
                if (tokens.length < 5 || tokens.length > 7) {
                    System.out.println("Invalid command");
                    System.out.println("Usage : !connect <driver> <host> <port> <database>");
                    System.out.println("        !connect <driver> <host> <port> <database> <user> <password>");
                    System.out.println("Example : !connect kairos localhost 5000 mydatabase root root");
                }

                System.out.println("Connecting to database...");
                if (tokens[1].equals("kairos")) {
                    // Register JDBC driver
                    driver = "jdbc:traindb:kairos";
                } else if (tokens[1].equals("traindb")) {
                    // Register JDBC driver
                    driver = "jdbc:traindb";
                } else {
                    System.out.println("Invalid driver");
                }

                String url = driver + "://" + tokens[2] + ":" + tokens[3] + "/" + tokens[4];
                if (tokens.length < 7) {
                    System.out.print("Enter user : ");
                    String USER = scanner.nextLine();
                    System.out.print("Enter password : ");
                    String PASS = scanner.nextLine();
                    conn = DriverManager.getConnection(url, USER, PASS);
                } else {
                    conn = DriverManager.getConnection(url, tokens[5], tokens[6]);
                }
                stmt = conn.createStatement();
                isConnected = true;
                System.out.println("Connection Success !!!");
                break;
            case "disconnect":
                if (!isConnected) {
                    System.out.println("Not connected to database");
                } else {
                    if (rs != null && !rs.isClosed()) {
                        rs.close();
			rs = null;
                    }
                    if (stmt != null && !stmt.isClosed()) {
                        stmt.close();
			stmt = null;
                    }
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
			conn = null;
                    }
		    isConnected = false;
                    System.out.println("Disconnected from database");
                }
                break;
            case "moreresult":
                if (tokens[1].equals("on")) {
                    isMoreResult = true;
                } else if (tokens[1].equals("off")) {
                    isMoreResult = false;
                } else {
                    System.out.println("Invalid command");
                    System.out.println("Usage : !moreresult <on|off>");
                }
                break;
            case "help":
                printHelp();
                break;
            default:
                System.out.println("Invalid command");
                break;
        }
    }

    public void handleResult() throws SQLException {
        rs = stmt.getResultSet();
        // Print results
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        // Calculate column width
        int[] columnWidth = new int[columnsNumber];
        for (int i = 1; i <= columnsNumber; i++) {
            int displaySize = rsmd.getColumnDisplaySize(i);
            int nameSize = rsmd.getColumnName(i).length();
            if (displaySize > nameSize)
                columnWidth[i - 1] = displaySize;
            else
                columnWidth[i - 1] = nameSize;
        }

        // Print column headers
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1)
                System.out.print(" | ");
            String columnName = rsmd.getColumnName(i);
            System.out.print(String.format("%-" + columnWidth[i - 1] + "s", columnName));
        }
        System.out.println();

        String output = "";

        // Print rows
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(" | ");
                }

                int columnType = rsmd.getColumnType(i);

                switch (columnType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        output = String.format("%-" + columnWidth[i - 1] + "s", rs.getString(i));
                        break;
                    case Types.INTEGER:
                        output = String.format("%-" + columnWidth[i - 1] + "d", rs.getInt(i));
                        break;
                    case Types.BOOLEAN:
                        output = String.format("%-" + columnWidth[i - 1] + "b", rs.getBoolean(i));
                        break;
                    case Types.DATE:
                        Date date = rs.getDate(i);
                        output = String.format("%-" + columnWidth[i - 1] + "s", date.toString());
                        break;
                    case Types.TIME:
                        Time time = rs.getTime(i);
                        output = String.format("%-" + columnWidth[i - 1] + "s", time.toString());
                        break;
                    case Types.TIMESTAMP:
                        Timestamp ts = rs.getTimestamp(i);
                        output = String.format("%-" + columnWidth[i - 1] + "s", ts.toString());
                        break;
                    case Types.FLOAT:
                        output = String.format("%-" + columnWidth[i - 1] + "f", rs.getFloat(i));
                        break;
                    case Types.DOUBLE:
                        output = String.format("%-" + columnWidth[i - 1] + "f", rs.getDouble(i));
                        break;
                    case Types.BIT:
                    case Types.CLOB:
                    case Types.BLOB:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        output = String.format("%-" + columnWidth[i - 1] + "s", rs.getBytes(i).toString());
                        break;
                    default:
                        output = String.format("%-" + columnWidth[i - 1] + "s", rs.getString(i));
                        break;
                }
                System.out.print(output);
            }
            System.out.println();
        }
    }

    public void msgLoop() {
        try {
            boolean result = false;
            // Main query execute loop
            while (true) {
                // Prompt user for query
                System.out.print("Enter query : ");
		String query = "";
	        query = scanner.nextLine();
		if (query.endsWith(";")) {
                    query = query.substring(0, query.length()-1);
		}

                if (query.startsWith("!")) {
                    query = query.substring(1);
                    try {
                        processCommands(query);
                    } catch (ClassNotFoundException e) {
                        System.out.println("Driver not found");
                    } catch (SQLException e) {
                        System.out.println("Connection failed");
                    }
                    continue;
                }

                if(isConnected == false) {
                    System.out.println("Not connected to database");
                    continue;
                }

                // Execute query
                try {
                    result = stmt.execute(query);
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    continue;
                }
                while (true) {
                    if (result) {
                        handleResult();
                    } else {
                        int updateCount = stmt.getUpdateCount();
                        if (updateCount > 0)
                            System.out.println(updateCount + " rows affected");
                        else
                            System.out.println("SQL Execute Success");
                    }
                    if (isMoreResult == false || ( result = stmt.getMoreResults()) == false)
                    {
                        System.out.println("getMoreReuslts() returned false");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                if (scanner != null)
                    scanner.close();
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {
        Tisql tisql = new Tisql();
        tisql.msgLoop();
    }

}
