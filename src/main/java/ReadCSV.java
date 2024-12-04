import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ReadCSV {
    public static void main(String[] args) {
        String csvFilePath = "C:/Users/joelo/IdeaProjects/DataEngProject/src/main/java/ML/movie.csv";
        int rowsToRead = 5;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            int rowCount = 0;

            // Read the header line (if present) and print it
            if ((line = br.readLine()) != null) {
                System.out.println("Header: " + line); // Print header
                rowCount++;
            }

            // Read the first 5 rows after the header
            while ((line = br.readLine()) != null && rowCount < rowsToRead) {
                System.out.println("Row " + rowCount + ": " + line);
                rowCount++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
