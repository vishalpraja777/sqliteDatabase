package net.sqlitetutorial;

import java.sql.DatabaseMetaData;  
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author sqlitetutorial.net
 */
public class Connect {
     /**
     * Connect to a sample database
     */

    private Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:D://sqlite/db/movies.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }


    public static void createNewDatabase(String fileName) {  
   
        String url = "jdbc:sqlite:D:/sqlite/db/" + fileName;  
   
        try {  
            Connection conn = DriverManager.getConnection(url);  
            if (conn != null) {  
                DatabaseMetaData meta = conn.getMetaData();  
                System.out.println("The driver name is " + meta.getDriverName());  
                System.out.println("A new database has been created.");  
            }  
   
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        }  
    }  


    public static void createNewTable() {
        // SQLite connection string
        String url = "jdbc:sqlite:D:/sqlite/db/movies.db";
        
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS movies (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	movies text NOT NULL,\n"
                + "	actor text NOT NULL,\n"
                + "	actress text NOT NULL,\n"
                + "	director text NOT NULL,\n"
                + "	year integer NOT NULL\n"
                + ");";

        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insert(int id, String movies, String actor, String actress, String director, int year) {
        String sql = "INSERT INTO movies(id,movies,actor,actress,director,year) VALUES(?,?,?,?,?,?)";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            pstmt.setString(2, movies);
            pstmt.setString(3, actor);
            pstmt.setString(4, actress);
            pstmt.setString(5, director);
            pstmt.setInt(6, year);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // connect();
        createNewDatabase("movies.db"); 
        createNewTable();
        Connect app = new Connect();
        app.insert(1,"Bahubabli","Yash","Srinidhi","Prashanth",2018);
        app.insert(2,"Bahubabli2","Yash","Srinidhi","Prashanth",2022);
        app.insert(3,"Dangal","Aamir","Fatima","Nitesh",2016);
    }
}