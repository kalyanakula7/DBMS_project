package org.example;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class ACIDPostgreSQLImplementation {

    public static void main(String[] args) throws SQLException,
            ClassNotFoundException {

        // Load the Postgres driver
        Class.forName("org.postgresql.Driver");


        // Connect to the default database with credentials
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "4095");

        // Atomicity
        conn.setAutoCommit(false);

        // For Isolation
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Statement stmt1 = null;
        try {
            // Create statement object
            stmt1 = conn.createStatement();

            // Either all the statements will execute or none. This is Atomicity.

            // Creating Tables
            stmt1.executeUpdate("CREATE TABLE IF NOT EXISTS Product (" +
                    "prod_id CHAR(10), " +
                    "pname VARCHAR(30), " +
                    "price DECIMAL, " +
                    "PRIMARY KEY (prod_id)" +
                    ")");


            stmt1.executeUpdate("CREATE TABLE IF NOT EXISTS Depot (" +
                    "dep_id CHAR(10), " +
                    "addr VARCHAR(30), " +
                    "volume INTEGER, " +
                    "PRIMARY KEY (dep_id)" +
                    ")");
//
//
            stmt1.executeUpdate("CREATE TABLE Stock (" +
                    "prod_id CHAR(10), " +
                    "dep_id CHAR(10), " +
                    "quantity NUMERIC, " +
                    "PRIMARY KEY(prod_id, dep_id), "+
                    "FOREIGN KEY(prod_id) REFERENCES Product(prod_id) ON DELETE CASCADE ON UPDATE CASCADE, " +
                    "FOREIGN KEY(dep_id) REFERENCES Depot(dep_id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ")");

//
//
//            // Insert Data
//
            stmt1.executeUpdate("INSERT INTO Product (prod_id, pname, price) VALUES " +
                    "('p1','tape',2.5)," +
                    "('p2', 'tv', 250)," +
                    "('p3', 'vcr', 80);" +
                    "");
//
//
            stmt1.executeUpdate("INSERT INTO Depot (dep_id, addr, volume) VALUES " +
                    "('d1', 'New York', 9000)," +
                    "('d2', 'Syracuse', 6000)," +
                    "('d4', 'New York', 2000);" +
                    "");
//
//
            stmt1.executeUpdate("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES " +
                    "('p1', 'd1', 1000), " +
                    "('p1', 'd2', -100), " +
                    "('p1', 'd4', 1200), " +
                    "('p3', 'd1', 3000), " +
                    "('p3', 'd4', 2000), " +
                    "('p2', 'd4', 1500), " +
                    "('p2', 'd1', -400), " +
                    "('p2', 'd2', 2000);" +
                    "");
//
//            // Transaction 2 -  The depot d1 is deleted from Depot and Stock
            stmt1.executeUpdate("DELETE FROM Depot WHERE dep_id = 'd1'");
//
//            // Transaction 4 - The depot d1 changes its name to dd1 in Depot and Stock.
            stmt1.executeUpdate("UPDATE Depot SET dep_id='dd1' WHERE dep_id = 'd1'");
//
//
            stmt1.executeUpdate("INSERT INTO Depot VALUES ('d100', 'Chicago', 100)");
            stmt1.executeUpdate("INSERT INTO Stock VALUES ('p1', 'd100', 150)");


        } catch (SQLException e) {
            System.out.println("An exception was thrown");

            // For Atomicity
            conn.rollback();
            stmt1.close();
            conn.close();
            return;
        }
        conn.commit();
        stmt1.close();
        conn.close();
    }
}
