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
    private boolean isFirstResult = true;

    public Tisql() {
        scanner = new Scanner(System.in);
        try {
            Class.forName(DRIVER);
        } catch (Exception e) {
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

    public String getDriverName(String databaseName) {
        switch(databaseName) {
            case "traindb":
                return "jdbc:traindb";
            case "kairos":
                return "jdbc:traindb:kairos";
            case "mysql":
                return "jdbc:traindb:mysql";
            case "altibase":
                return "jdbc:traindb:altibase";
            case "tibero":
                return "jdbc:traindb:tibero";
            case "postgresql":
                return "jdbc:traindb:postgresql";
            default:
                return "invalid driver";
        }
    }

    public void connectDBMS(String[] tokens) throws SQLException {
        String driver = null;
        if (tokens.length < 5 || tokens.length > 7) {
            System.out.println("Invalid command");
            System.out.println("Usage : !connect <driver> <host> <port> <database>");
            System.out.println("        !connect <driver> <host> <port> <database> <user> <password>");
            System.out.println("Example : !connect kairos localhost 5000 mydatabase root root");

            return;
        }

        System.out.println("Connecting to database...");
        driver = getDriverName(tokens[1]);

        String url = null;
        int offset = 0;
        if (tokens[1].equals("postgresql")) {
            if (tokens.length == 4 || tokens.length == 6)
                url = driver + "://" + tokens[2] + "/" + tokens[3];
            else {
                offset=1;
                url = driver + "://" + tokens[2] + ":" + tokens[3] + "/" + tokens[4];
            }
        } else {
            offset = 1;
            if(tokens[1].equals("tibero")) {
                url = driver + ":thin:@" + tokens[2] + ":" + tokens[3] + ":" + tokens[4];
            } else {
                url = driver + "://" + tokens[2] + ":" + tokens[3] + "/" + tokens[4];
            }
        }

        if (tokens.length < 6) {
            System.out.print("Enter user : ");
            String USER = scanner.nextLine();
            System.out.print("Enter password : ");
            String PASS = scanner.nextLine();
            conn = DriverManager.getConnection(url, USER, PASS);
        } else {
            conn = DriverManager.getConnection(url, tokens[offset + 4], tokens[offset + 5]);
        }
        stmt = conn.createStatement();
        isConnected = true;
        System.out.println("Connection Success !!!");
    }

    public void disconnectDBMS() throws SQLException {
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
                connectDBMS(tokens);
                break;
            case "disconnect":
                disconnectDBMS();
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

        if(isFirstResult) {
            for (int i = 1; i <= columnsNumber; i++) {
                    System.out.print("+");
                for (int j = 0; j < columnWidth[i - 1]; j++) {
                    System.out.print("-");
                }
            }
            System.out.println("+");
            // Print column headers
            for (int i = 1; i <= columnsNumber; i++) {
                System.out.print("|");
                String columnName = rsmd.getColumnName(i);
                System.out.print(String.format("%-" + columnWidth[i - 1] + "s", columnName));
            }
            System.out.println("|");
            isFirstResult = false;

            for (int i = 1; i <= columnsNumber; i++) {
                System.out.print("+");
                for (int j = 0; j < columnWidth[i - 1]; j++) {
                    System.out.print("-");
                }
            }
            System.out.println("+");
        }

        String output = "";

        // Print rows
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                
                System.out.print("|");
                
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
                    case Types.BIGINT:
                        output = String.format("%-" + columnWidth[i - 1] + "d", rs.getLong(i));
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
            System.out.println("|");
        }
        for (int i = 1; i <= columnsNumber; i++) {
            System.out.print("+");
            for (int j = 0; j < columnWidth[i - 1]; j++) {
                System.out.print("-");
            }
        }
        System.out.println("+");
    }

    public void msgLoop() {
        try {
            boolean result = false;
            int msgCnt = 0;
            // Main query execute loop
            while (true) {
                // Prompt user for query
                System.out.print("Tisql> ");
                String query = "";
                query = scanner.nextLine();
                if (query.endsWith(";")) {
                    query = query.substring(0, query.length() - 1);
                }

                if (query.isEmpty() || query.length() == 0) {
                    continue;
                }

                if (query.compareTo("exit") == 0)
                {
                    System.out.println("Exit command");
                    System.exit(0);
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

                if (isConnected == false) {
                    System.out.println("Not connected to database");
                    continue;
                }

                // Execute query
                long startTime = System.currentTimeMillis();
                try {
                    result = stmt.execute(query);
                    msgCnt++;
                    isFirstResult = true;
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    continue;
                }
                while (true) {
                    if (result) {
                        if(isMoreResult == false && msgCnt == 3) {
                            Thread.sleep(600);
                        }
                        handleResult();
                    } else {
                        int updateCount = stmt.getUpdateCount();
                        if (updateCount > 0)
                            System.out.print(updateCount + " rows affected");
                        else
                            System.out.print("SQL Execute Success");
                    }
                    if (isMoreResult == false || (result = stmt.getMoreResults()) == false) {
                        break;
                    }
                    Thread.sleep(75);
                }
                long endTime = System.currentTimeMillis();
                System.out.println(" (" + ((double)(endTime - startTime)/1000) + " sec)");
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
