package org.kuali.student.dbdiff;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Establish a database connection.
 */
public class DbConnection {

    private Connection connection;

    private String schemaName;

    public DbConnection(String driverString, String url, String schemaName, String password) throws ClassNotFoundException, SQLException {
        this.schemaName = schemaName;
        // Register JDBC driver
        Class.forName(driverString);
        connection = DriverManager.getConnection(url, schemaName, password);

    }

    public Connection getConnection() {
        return connection;
    }

    public String getSchemaName() {
        return schemaName;
    }
}
