import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadTestData {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:database_test.db"; // Path to the DuckDB database file
        String csvFilePath = "C:/Users/joelo/IdeaProjects/DataEngProject/src/main/java/test.csv"; // Path to test.csv

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Drop the table if it exists to ensure a clean start
            stmt.execute("DROP TABLE IF EXISTS Movie_test;");

            // Create table schema to match the CSV file structure
            stmt.execute("CREATE TABLE IF NOT EXISTS Movie_test ("
                    + "movieId INTEGER, "
                    + "title VARCHAR, "
                    + "genres VARCHAR);");

            // Use COPY command to load data from test.csv into Movie_test
            String copyCSVSQL = "COPY Movie_test (movieId, title, genres) "
                    + "FROM '" + csvFilePath + "' "
                    + "(DELIMITER ',', HEADER TRUE, QUOTE '\"', ESCAPE '\\', NULL 'NULL', SKIP 0, IGNORE_ERRORS TRUE);";
            stmt.execute(copyCSVSQL);
            System.out.println("Data successfully loaded into Movie_test table.");

            // Check if data was loaded by counting rows
            ResultSet countResult = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM Movie_test;");
            if (countResult.next()) {
                int rowCount = countResult.getInt("rowcount");
                System.out.println("Rows loaded: " + rowCount);
            }

            // Query the data to verify and print results
            ResultSet resultSet = stmt.executeQuery("SELECT * FROM Movie_test;");
            while (resultSet.next()) {
                int movieId = resultSet.getInt("movieId");
                String title = resultSet.getString("title");
                String genres = resultSet.getString("genres");

                System.out.printf("Movie ID: %d, Title: %s, Genres: %s%n", movieId, title, genres);
            }

            countResult.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
