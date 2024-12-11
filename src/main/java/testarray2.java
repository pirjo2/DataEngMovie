import java.sql.*;
import org.json.JSONArray;  // Import JSON library to handle JSON arrays

public class testarray2 {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:star_schema.db"; // Persistent DuckDB database

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // SQL command to drop the table
            String dropTableQuery = "DROP TABLE IF EXISTS RatingFact;";

            // Execute the query
            stmt.executeUpdate(dropTableQuery);
            System.out.println("Table GenreDim dropped successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();  // Handle other exceptions like JSON parsing
        }
    }
}
