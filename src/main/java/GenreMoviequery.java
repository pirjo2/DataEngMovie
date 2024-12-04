import java.sql.*;
import org.json.JSONArray;  // Import JSON library to handle JSON arrays

public class GenreMoviequery {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:star_schema.db"; // Persistent DuckDB database

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Query all films and their associated genres
            String querySQL = """
                SELECT FilmID, Genres
                FROM RatingFact;
            """;

            ResultSet resultSet = stmt.executeQuery(querySQL);

            // Prepare a structure to hold genre associations
            // Use a HashMap to store genres and associated FilmIDs
            java.util.Map<String, java.util.List<Integer>> genreToFilms = new java.util.HashMap<>();

            // Process each row
            while (resultSet.next()) {
                int filmId = resultSet.getInt("FilmID");
                String genresJson = resultSet.getString("Genres");

                // Parse the JSON array of genres
                JSONArray genresArray = new JSONArray(genresJson);

                // For each genre, associate the filmID with it
                for (int i = 0; i < genresArray.length(); i++) {
                    String genre = genresArray.getString(i);

                    // If the genre is not in the map, add it
                    genreToFilms.putIfAbsent(genre, new java.util.ArrayList<>());
                    genreToFilms.get(genre).add(filmId);  // Add the filmID to the genre list
                }
            }
            resultSet.close();

            // Now print the results
            for (String genre : genreToFilms.keySet()) {
                System.out.printf("Genre: %s, FilmIDs: %s%n", genre, genreToFilms.get(genre));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();  // Handle other exceptions like JSON parsing
        }
    }
}
