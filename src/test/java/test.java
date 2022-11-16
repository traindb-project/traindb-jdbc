import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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

			/*
			Statement stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT * FROM tb_patient LIMIT 3");

			while (rs.next()) {
				System.out.println(rs.getString(1) + " - " +rs.getString(2));
			}
			*/
			
			PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM tb_patient WHERE patientNo = ? AND patientName = ?");
			pstmt.setInt(1, 1234567);
			pstmt.setString(2, "홍길동");
			ResultSet rs = pstmt.executeQuery();
			
			while (rs.next()) {
				System.out.println(rs.getString(1) + " - " +rs.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}