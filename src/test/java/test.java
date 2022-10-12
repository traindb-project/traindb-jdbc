import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class test {
	public static final String DEFAULT_NON_PROXY_HOSTS = "localhost|127.*|[::1]|0.0.0.0|[::0]";

	public static void main(String[] args) throws ClassNotFoundException {
		System.setProperty("jdbc.drivers", "traindb.jdbc.Driver");

		try {
			PrintWriter pw = new PrintWriter(System.out);
			DriverManager.setLogWriter(pw);

			Connection conn = DriverManager.getConnection(
					"jdbc:traindb:mysql://onwards.iptime.org:17925/ecrf?serverTimezone=UTC&characterEncoding=UTF-8&allowMultiQueries=true",
					"root", "dhsdnjwm@2022");

			// System.out.println(conn.getClientInfo());

			Statement stmt = conn.createStatement();

			// System.out.println(stmt);

			ResultSet rs = stmt.executeQuery("SELECT * FROM tb_patient LIMIT 3");

			// PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO
			// tb_patient VALUES (default, ?, ?, ?, ? , ?, ?)");
			// preparedStatement.setString(1, "Test");

			// preparedStatement.executeQuery();

			while (rs.next()) {
				System.out.println(rs.getString(1) + " - " +rs.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}