import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class loaddata {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:database_new.db"; // Persistent database
        String csvFilePath = "src/main/java/ML/movie.csv";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS Movie_dimension;");
            // Ensure that the Movie_dimension table exists with appropriate schema
            stmt.execute("CREATE TABLE IF NOT EXISTS Movie_dimension ("
                    + "id INTEGER, "
                    + "title VARCHAR, "
                    + "genres VARCHAR);");
                    //+ "IMDB_rating FLOAT);");

            // Delete existing data in the table without dropping it
            //stmt.execute("DELETE FROM Movie_dimension;");
            //System.out.println("Existing data in Movie_dimension has been deleted.");

            // Load data into Movie_dimension table from the CSV file
            String copyCSVSQL = "COPY Movie_dimension (id, title, genres) "
                    + "FROM '" + csvFilePath + "' "
                    + "(DELIMITER ',', HEADER TRUE, QUOTE '\"', ESCAPE '\\', NULL 'NULL', SKIP 0, IGNORE_ERRORS FALSE);";
            stmt.execute(copyCSVSQL);
            System.out.println("Data successfully loaded into Movie_dimension table.");

            // Check if data was loaded by counting rows
            ResultSet countResult = stmt.executeQuery("SELECT COUNT(*) AS rowcount FROM Movie_dimension;");
            if (countResult.next()) {
                int rowCount = countResult.getInt("rowcount");
                System.out.println("Rows loaded: " + rowCount);
            }

            // Query the data to verify and print results
            ResultSet resultSet = stmt.executeQuery("SELECT * FROM Movie_dimension LIMIT 5;");
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String title = resultSet.getString("title");
                String genres = resultSet.getString("genres");
                //float imdbRating = resultSet.getFloat("IMDB_rating");

                System.out.printf("Movie ID: %d, Title: %s, Genres: %s",
                        id, title, genres);
            }

            countResult.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
