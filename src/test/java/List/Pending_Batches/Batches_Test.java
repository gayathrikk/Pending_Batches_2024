package List.Pending_Batches;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;


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
        String query =  "SELECT " +
                "sb.id AS slidebatch_id, " +
                "sb.name AS name, " +
                "s.filename AS filename " +
                "FROM slidebatch sb " +
                "LEFT JOIN slide s ON sb.id = s.slidebatch " +
                "WHERE sb.process_status = 8 " +
                "AND s.jp2Path LIKE '%" + jp2Path + "%';";
        ResultSet resultSet = statement.executeQuery(query);

        Set<Integer> distinctBatchIds = new LinkedHashSet<>();

        while (resultSet.next()) {
            Integer id = resultSet.getInt("slidebatch_id");
            String name = resultSet.getString("name");
            String filename = resultSet.getString("filename");
            distinctBatchIds.add(id);
            System.out.println("ID: " + id + ", Name: " + name + ", Filename: " + filename);
        }

        // Close resources
        resultSet.close();
        statement.close();

        return distinctBatchIds.size();
    }
}