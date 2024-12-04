import java.sql.*;
import org.json.JSONArray;

public class testarray {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:star_schema.db"; // Persistent DuckDB database

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Create tables
            String createGenreDim = """
                CREATE TABLE IF NOT EXISTS GenreDim (
                    GenreID INTEGER PRIMARY KEY,
                    GenreName TEXT
                );
            """;
            stmt.execute(createGenreDim);

            String createRatingFact = """
                CREATE TABLE IF NOT EXISTS RatingFact (
                    FilmID INTEGER PRIMARY KEY,
                    Rating FLOAT,
                    Genres JSON
                );
            """;
            stmt.execute(createRatingFact);

            System.out.println("Tables created successfully.");

            // Clear existing data
            stmt.execute("DELETE FROM GenreDim;");
            stmt.execute("DELETE FROM RatingFact;");

            System.out.println("Old data cleared from tables.");

            // Insert sample data into GenreDim
            String insertGenres = """
                INSERT INTO GenreDim (GenreID, GenreName) VALUES
                (1, 'Action'),
                (2, 'Comedy'),
                (3, 'Horror'),
                (4, 'Thriller'),
                (5, 'Drama');
            """;
            stmt.execute(insertGenres);

            System.out.println("Sample data inserted into GenreDim.");

            // Insert sample data into RatingFact
            String insertRatings = """
                INSERT INTO RatingFact (FilmID, Rating, Genres) VALUES
                (1, 4.5, '["Action", "Comedy"]'),
                (2, 3.0, '["Horror", "Thriller"]'),
                (3, 4.0, '["Drama"]'),
                (4, 4.7, '["Action", "Drama"]'),
                (5, 2.5, '["Comedy", "Horror"]');
            """;
            stmt.execute(insertRatings);

            System.out.println("Sample data inserted into RatingFact.");

            // Query the data using json_extract to match genres
            String querySQL = """
                SELECT rf.FilmID, rf.Rating, gd.GenreName
                FROM RatingFact rf
                JOIN GenreDim gd ON 
                json_extract(rf.Genres, '$') LIKE '%' || gd.GenreName || '%'
                LIMIT 10;
            """;

            ResultSet resultSet = stmt.executeQuery(querySQL);

            // Print the results
            while (resultSet.next()) {
                int filmId = resultSet.getInt("FilmID");
                float rating = resultSet.getFloat("Rating");
                String genreName = resultSet.getString("GenreName");

                System.out.printf("FilmID: %d, Rating: %.1f, Genre: %s%n", filmId, rating, genreName);
            }
            resultSet.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
