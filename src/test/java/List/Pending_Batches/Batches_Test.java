package List.Pending_Batches;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

import org.testng.annotations.Test;

public class Batches_Test {

    @Test
    public void testDB() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("Driver loaded");

        String url = "jdbc:mysql://apollo2.humanbrain.in:3306/HBA_V2";
        String username = "root";
        String password = "Health#123";
        Connection connection = DriverManager.getConnection(url, username, password);
        System.out.println("MYSQL database connect");

        // Define an array of jp2Path values to loop through
        String[] jp2Paths = {"pp1", "pp2", "pp3", "pp4", "pp5", "pp7", "pp5d2"};

        for (int i = 0; i < jp2Paths.length; i++) {
            int count = processQuery(connection, jp2Paths[i]);
            System.out.println("Total Pending Batches of " + jp2Paths[i] + ": " + count);
            
            // Add extra spaces between specific jp2Path values
            if (i < jp2Paths.length - 1) {
                System.out.println(); // Add an empty line for spacing
            }
        }

        // Close the database connection
        connection.close();
    }

    private int processQuery(Connection connection, String jp2Path) throws SQLException {
        Statement statement = connection.createStatement();
        String query = "SELECT id, name\n" +
                "FROM HBA_V2.slidebatch\n" +
                "WHERE id IN (\n" +
                "SELECT slidebatch\n" +
                "FROM HBA_V2.slide\n" +
                "WHERE jp2Path LIKE '%" + jp2Path + "%'\n" +
                ") and process_status = 8;";
        ResultSet resultSet = statement.executeQuery(query);

        int count = 0;

        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            System.out.println("id: " + id + ", name: " + name);
            count++;
        }

        // Close resources
        resultSet.close();
        statement.close();

        return count;
    }
}
