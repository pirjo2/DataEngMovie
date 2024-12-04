import java.sql.*;
import org.json.JSONArray;  // Import JSON library to handle JSON arrays

public class testarray2 {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:star_schema.db"; // Persistent DuckDB database

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Query raw data from RatingFact
            String queryRawData = """
                SELECT FilmID, Rating, Genres
                FROM RatingFact
                LIMIT 10;
            """;

            ResultSet resultSet = stmt.executeQuery(queryRawData);

            // Print the raw data and process genres
            while (resultSet.next()) {
                int filmId = resultSet.getInt("FilmID");
                float rating = resultSet.getFloat("Rating");
                String genresJson = resultSet.getString("Genres");

                // Print the raw data
                System.out.printf("FilmID: %d, Rating: %.1f, Genres (raw): %s%n", filmId, rating, genresJson);

                // Parse the JSON array and display genres prettily
                JSONArray genresArray = new JSONArray(genresJson);
                System.out.print("Genres: ");
                for (int i = 0; i < genresArray.length(); i++) {
                    System.out.print(genresArray.getString(i));
                    if (i < genresArray.length() - 1) {
                        System.out.print(", ");  // Add comma between genres
                    }
                }
                System.out.println();  // Move to the next line after printing genres
            }
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();  // Handle other exceptions like JSON parsing
        }
    }
}
