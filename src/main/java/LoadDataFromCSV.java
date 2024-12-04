import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.sql.*;

public class LoadDataFromCSV {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:database.db"; // Make sure this is a persistent database
        String csvFilePath = "C:/Users/joelo/IdeaProjects/DataEngProject/src/main/java/movie.csv";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Load data into Movie_dimension table from movies.csv
            //String loadMovies = "COPY Movie_dimension FROM 'src/main/java/movie.csv' (DELIMITER ',');";
            //stmt.execute(loadMovies);

            // Load data into User_dimension table from users.csv
            //String loadUsers = "COPY User_dimension FROM 'path/to/users.csv' (DELIMITER ',');";
            //stmt.execute(loadUsers);

            System.out.println("Data loaded successfully from CSV files.");

            // Explicitly set the CSV reading options for delimiter, quote, and escape
            String copyCSVSQL = "COPY Movie_dimension (id, title, release_year) "
                    + "FROM '" + csvFilePath + "' "
                    + "(DELIMITER ',', HEADER TRUE, QUOTE '\"', ESCAPE '\\', NULL 'NULL', SKIP 1, IGNORE_ERRORS TRUE);";
            stmt.execute(copyCSVSQL);

            System.out.println("Data successfully loaded into Movie_dimension table.");

            // Query the data
            String querySQL = "SELECT * FROM Movie_dimension LIMIT 5;";  // Modify the LIMIT as needed
            ResultSet resultSet = stmt.executeQuery(querySQL);

            // Loop through and print the results
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String title = resultSet.getString("title");
                int releaseYear = resultSet.getInt("release_year");
                float imdbRating = resultSet.getFloat("IMDB_rating");

                System.out.printf("Movie ID: %d, Title: %s, Release Year: %d, IMDB Rating: %.1f%n",
                        id, title, releaseYear, imdbRating);
            }
            resultSet.close();

            // Close the connection
            conn.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
