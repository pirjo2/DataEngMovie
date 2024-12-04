import java.sql.*;

public class CreateStarSchemaTables {
    public static void main(String[] args) {
        String url = "jdbc:duckdb:database.db"; // database.db will be created in your project folder

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Create Tag_dimension table
            String createTagDimension = "CREATE TABLE IF NOT EXISTS Tag_dimension (" +
                    "id BIGINT PRIMARY KEY, " +
                    "tag_name VARCHAR" +
                    ");";
            stmt.execute(createTagDimension);

            // Create Genre_dimension table
            String createGenreDimension = "CREATE TABLE IF NOT EXISTS Genre_dimension (" +
                    "id BIGINT PRIMARY KEY, " +
                    "genre_name VARCHAR" +
                    ");";
            stmt.execute(createGenreDimension);

            // Create User_dimension table
            String createUserDimension = "CREATE TABLE IF NOT EXISTS User_dimension (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR" +
                    ");";
            stmt.execute(createUserDimension);

            // Create Movie_dimension table
            String createMovieDimension = "CREATE TABLE Movie_dimension (" +
                    "id INT PRIMARY KEY, " +
                    "title VARCHAR, " +
                    "release_year INT, " +
                    "IMDB_rating FLOAT" +
                    ");";
            stmt.execute(createMovieDimension);

            // Create Director_dimension table
            String createDirectorDimension = "CREATE TABLE IF NOT EXISTS Director_dimension (" +
                    "id BIGINT PRIMARY KEY, " +
                    "director VARCHAR" +
                    ");";
            stmt.execute(createDirectorDimension);

            // Create Date_dimension table
            String createDateDimension = "CREATE TABLE IF NOT EXISTS Date_dimension (" +
                    "id INT PRIMARY KEY, " +
                    "date DATE, " +
                    "day_of_week VARCHAR, " +
                    "month VARCHAR, " +
                    "year INT" +
                    ");";
            stmt.execute(createDateDimension);

            // Create Rating_fact table
            String createRatingFact = "CREATE TABLE IF NOT EXISTS Rating_fact (" +
                    "id INT PRIMARY KEY, " +
                    "user_ID INT REFERENCES User_dimension(id), " +
                    "movie_ID INT REFERENCES Movie_dimension(id), " +
                    "tag_ID BIGINT REFERENCES Tag_dimension(id), " +  // Updated to BIGINT
                    "rating_value FLOAT, " +
                    "genre_1_ID BIGINT REFERENCES Genre_dimension(id), " + // Updated to BIGINT
                    "genre_2_ID BIGINT REFERENCES Genre_dimension(id), " + // Updated to BIGINT
                    "genre_3_ID BIGINT REFERENCES Genre_dimension(id), " + // Updated to BIGINT
                    "director_ID BIGINT REFERENCES Director_dimension(id)" +
                    ");";
            stmt.execute(createRatingFact);

            System.out.println("Tables created successfully.");

        } catch (SQLException e) {
            e.printStackTrace();//
        }
    }
}
