package org.kuali.student.dbdiff;

import org.apache.commons.io.FileUtils;

import javax.activation.UnsupportedDataTypeException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbDiff {
    private static final String DRIVER_CLASS_NAME =  "oracle.jdbc.OracleDriver";
    private static final String JDBC_URL = "jdbc:oracle:thin:@oracle:1521:XE";
    private static final String FILE_OUTPUT_DIRECTORY = "target/sqls/";

    private DbConnection beforeDatabase; //  Connection to the database before changes were made.
    private DbConnection afterDatabase;

    //  These are database types that can't be handled.
    private static final List<String> UNSUPPORTED_TYPES = Arrays.asList(new String[] {"BLOB", "CLOB"});

    public static void main(String[] args) {
        //  TODO: Don't hard code the schema/passwd
        DbDiff dbDiff = new DbDiff("KSBUNDLEDNEW", "KSBUNDLEDNEW", "KSBUNDLED", "KSBUNDLED");
        dbDiff.doDiff();
    }

    public DbDiff(String beforeUser, String beforePass, String afterUser, String afterPass) {
        try {
            this.beforeDatabase = new DbConnection(DRIVER_CLASS_NAME, JDBC_URL, beforeUser, beforePass);
        } catch (Exception e) {
            throw new RuntimeException("Could not get connection to before database.", e);
        }

        try {
            this.afterDatabase = new DbConnection(DRIVER_CLASS_NAME, JDBC_URL, afterUser, afterPass);
        } catch (Exception e) {
            throw new RuntimeException("Could not get connection to after database.", e);
        }

        //  Cleanup the output directory from previous runs.
        File outputDir = new File(FILE_OUTPUT_DIRECTORY);
        if (outputDir.exists()) {
            try {
                FileUtils.deleteDirectory(outputDir);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Could not remove pre-existing [%s]: %s", FILE_OUTPUT_DIRECTORY, e.getLocalizedMessage()));
            }
        }

        //  Create a new output directory.
        boolean success = (new File(FILE_OUTPUT_DIRECTORY)).mkdirs();
        if (!success) {
            throw new RuntimeException("Count not create " + FILE_OUTPUT_DIRECTORY);
        }
    }

    public void doDiff() {
        List<String> changedTables = findTablesWithChanges(beforeDatabase, afterDatabase);
        Collections.sort(changedTables);

        /**
         * Get the columns that make up the primary keys for the tables that have changed.
         * Usually, this is simply ID, but sometimes there isn't a key (e.g. join tables)
         */
        List<DbTableMetadata> tables = new ArrayList<DbTableMetadata>();
        for (String tableName : changedTables) {
            DbTableMetadata table = new DbTableMetadata(tableName);
            try {
                findColumns(beforeDatabase, table);
            } catch (UnsupportedDataTypeException e) {
                System.err.println(e.getLocalizedMessage());
                continue;
            }
            tables.add(table);
        }

        /**
         * Now, for each table, using the key columns, get the key values for the missing rows.
         */
        for (DbTableMetadata table : tables) {
              DbResultSet keys = new DbResultSet(table); //  Storage for the keys of the new rows.
              findKeysForNewRows(beforeDatabase, afterDatabase, keys);
              output(afterDatabase, keys);
        }
        System.err.println("Done.");
    }

    /**
     * Outputs the result set for a given table.
     * @param afterDatabase
     * @param keys
     */
    private void output(DbConnection afterDatabase, DbResultSet keys) {
        FileOutputStream fileOut = null;
        String fileName = String.format("%s%s.sql", FILE_OUTPUT_DIRECTORY, keys.getTableMetadata().getTableName());
        System.err.println(String.format("Writing %s rows to %s", keys.getResults().size(), fileName));
        try {
            fileOut = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format("Could not open SQL file: %s", fileName));
        }

        //  Fill in the rest of the data (beyond the key(s)) for each row
        for (Map.Entry<String, Map<String, String>> row : keys.getResults().entrySet()) {
            Map<String, String> columns = row.getValue();
            populateRowData(afterDatabase, keys.getTableMetadata(), columns);
            try {
                outputAsSql(keys.getTableMetadata(), columns, fileOut);
            } catch (IOException e) {
                System.err.println(String.format("Failed to write state to file [%s]. %s", fileName, e.getLocalizedMessage()));
            }
        }

        // Clean up
        try {
            fileOut.flush();
            fileOut.close();
        } catch (IOException e) {
             System.err.println(String.format("File [%s] did not close gracefully. %s", fileName, e.getLocalizedMessage()));
        }
    }

    private void outputAsSql(DbTableMetadata tableMetadata, Map<String, String> columns, FileOutputStream fileOut) throws IOException {
        //  Create column and values statements.
        StringBuilder cols =  new StringBuilder();
        StringBuilder values =  new StringBuilder();
        //  Put the columns in alphabetical order.
        List<DbColumnMetadata> columnsList = new ArrayList(tableMetadata.getColumns());
        Collections.sort(columnsList);

        for (DbColumnMetadata column : columnsList) {
            String value = columns.get(column.getName());
            if (value != null) {
                if (values.length() != 0) {
                    values.append(",");
                    cols.append(",");
                }
                String type = column.getType();
                values.append(String.format(SqlValueEncloser.findFormat(type), value));
                cols.append(column.getName());
            }
        }

        //  Put it all together.
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableMetadata.getTableName()).append(" ");
        sql.append(String.format("(%s)\n  VALUES (%s)\n/\n", cols.toString(), values.toString()));

        fileOut.write(sql.toString().getBytes());
    }

    private String encloseValue(DbColumnMetadata key, String value) {
        return String.format(SqlValueEncloser.findFormat(key.getType()), value);
    }

    /**
     * Creates an SQL where clause to retrieve a specific row of data, either by the key column(s)
     * or by all columns if the table has no key.
     */
    private String makeWhereClauseForSingleRow(DbTableMetadata table, Map<String, String> columns) {
        StringBuilder where = new StringBuilder();
        for (DbColumnMetadata key : table.getKeyColumns()) {
            if (where.length() != 0) {
                where.append(" and ");
            }
            String colName = key.getName();
            where.append(String.format("%s=%s", key.getName(), encloseValue(key, columns.get(colName))));
        }
        return where.toString();
    }

    /**
     * Populates all data for a particular row of data.
     * @param connection The DB connection.
     * @param table The table metadata.
     * @param columns Contains the keys or column values for the row. Also, provides storage for the new values.
     */
    private void populateRowData(DbConnection connection, DbTableMetadata table, Map<String, String> columns) {
        String sql = String.format("select * from %s where %s",
                table.getTableName(), makeWhereClauseForSingleRow(table, columns));
        Statement statement = null;

        try {
            statement = connection.getConnection().createStatement();
            ResultSet rs = statement.executeQuery(sql);

            // TODO: This should check for multiple rows in the result set and throw and error.
            while (rs.next()) {
                for (DbColumnMetadata column : table.getColumns()) {
                    //  No need to populate the key data again.
                    if (column.isKey()) {
                        continue;
                    }
                    String value = rs.getString(column.getName());
                    //  If the value is null then no need to set it.
                    if (value == null) {
                        continue;
                    }
                    //  Single quotes have to be escaped as ''
                    if (value.contains("'")) {
                        value = value.replace("'", "''");
                    }
                    columns.put(column.getName(), value);
                }
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to populate row data.", e);
        }
    }

    /**
     * Populates the column info for a particular table (name, type, isKey) as DbColumnMetadatas   .
     * @param connection
     * @param table The DbTableMetadata to store the column info in.
     * @throws UnsupportedDataTypeException When a table contains a column that isn't supported (e.g. BLOBs)
     */
    private void findColumns(DbConnection connection, DbTableMetadata table) throws UnsupportedDataTypeException {
        //  First query for the columns that make up the key and save them.
        List<String> keyColumns = new ArrayList<String>();
        String sql = String.format("SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols " +
                "WHERE cols.table_name = '%s' " +
                "AND cons.constraint_type = 'P' " +
                "AND cons.constraint_name = cols.constraint_name " +
                "AND cons.owner = cols.owner " +
                "ORDER BY cols.table_name, cols.position", table.getTableName());

        try {
            Statement statement = connection.getConnection().createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                String name = rs.getString("column_name");
                keyColumns.add(name);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException("Query for keys blew up.", e);
        }

        //  Now find the rest of the column metadata and store it.
        sql = String.format("select * from %s where rownum = 1", table.getTableName());
        try {
            Statement s2 = connection.getConnection().createStatement();
            ResultSet rs = s2.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            for (int i = 1; i < columnCount + 1; i++) {
                String name = resultSetMetaData.getColumnName(i);
                String type = resultSetMetaData.getColumnTypeName(i);
                if (UNSUPPORTED_TYPES.contains(type)) {
                    throw new UnsupportedDataTypeException(String.format("Table %s column %s is of type %s and currently not supported.",
                            table.getTableName(), name, type));
                }
                boolean isKeyColumn = false;
                if (keyColumns.contains(name)) {
                    isKeyColumn = true;
                }
                DbColumnMetadata column = new DbColumnMetadata(name, type, isKeyColumn);
                table.getColumns().add(column);
            }
            rs.close();
            s2.close();
        } catch (SQLException e) {
            throw new RuntimeException("Query for columns blew up.", e);
        }
    }

    /**
     * Diffs the keys in the before and after table to determine which rows were added.
     * @param beforeDatabase The DB connection for the "before" database.
     * @param afterDatabase  The DB connection for the "before" database.
     * @param keyResults  Storage for the keys of the new rows.
     */
    private void findKeysForNewRows(DbConnection beforeDatabase, DbConnection afterDatabase, DbResultSet keyResults) {
        DbResultSet beforeKeys = new DbResultSet(keyResults.getTableMetadata());
        DbResultSet afterKeys = new DbResultSet(keyResults.getTableMetadata());

        findKeysForTable(beforeDatabase, beforeKeys);
        findKeysForTable(afterDatabase, afterKeys);

        //  See which keys rows have been added
        for (Map.Entry<String, Map<String, String>> row : afterKeys.getResults().entrySet()) {
            if ( ! beforeKeys.getResults().containsKey(row.getKey())) {
                keyResults.addResult(row.getKey(), row.getValue());
            }
        }
    }

    /**
     * Populates a DBResultSet with the keys of all the rows from the given database and table.
     * @param connection A database connection
     * @param results Storage for the results.
     */
    private void findKeysForTable(DbConnection connection, DbResultSet results) {
        // TODO: Optimization. Only select needed columns.
        String sql = String.format("select * from %s", results.getTableMetadata().getTableName());
        Statement statement = null;

        List<DbColumnMetadata> orderedKeyColumns = new ArrayList<DbColumnMetadata>(results.getTableMetadata().getKeyColumns());
        Collections.sort(orderedKeyColumns);

        try {
            statement = connection.getConnection().createStatement();
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                //  Get the values for each of the key columns
                Map<String, String> keyValues = new HashMap<String, String>();
                for (DbColumnMetadata column : orderedKeyColumns) {
                    keyValues.put(column.getName(), rs.getString(column.getName()));
                }

                //  Build id for the row by concatenating all of the column values into a single string.
                StringBuffer key = new StringBuffer();
                for (DbColumnMetadata col : orderedKeyColumns) {
                    key.append(keyValues.get(col.getName()));
                }

                results.addResult(key.toString(), keyValues);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Could not fetch keys for table ", results.getTableMetadata().getTableName()), e);
        }
    }

    /**
     * Examines table row counts to determine which tables have had rows added.
     * @param beforeDatabase A database connection to the clean database.
     * @param afterDatabase  A database connection to the updated database.
     * @return  A list of table names which have new rows of data.
     */
    private List<String> findTablesWithChanges(DbConnection beforeDatabase, DbConnection afterDatabase) {
        System.err.println(String.format("Getting row counts."));
        List<String> tables = new ArrayList<String>();

        Map<String, Integer> beforeCounts = findRowCounts(beforeDatabase);
        Map<String, Integer> afterCounts = findRowCounts(afterDatabase);
        for (Map.Entry<String, Integer> entry: beforeCounts.entrySet()) {
            String tableName = entry.getKey();
            Integer beforeCount = entry.getValue();
            Integer afterCount = afterCounts.get(tableName);
            if (afterCount > beforeCount) {
                tables.add(tableName);
            }
        }
        System.err.println(String.format("Examined %s tables. %s contained new rows.", beforeCounts.size(), tables.size()));
        return tables;
    }

    /**
     * Gets the row count for all tables in a schema. Since the ALL_TABLES row count is only populated if stats
     * are being generated and aren't 100% accurate we'll do it the hard way ... select count().
     * @param connection
     * @return
     */
    private Map<String, Integer> findRowCounts(DbConnection connection) {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        String sql = String.format("select TABLE_NAME from ALL_TABLES where OWNER = '%s'", connection.getSchemaName());
        Statement statement = null;
        try {
            statement = connection.getConnection().createStatement();
            ResultSet rs = statement.executeQuery(sql);

             while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                counts.put(tableName, 0);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Could not get row counts for schema: %", connection.getSchemaName()), e);
        }

        for (String tableName : counts.keySet()) {
            sql = String.format("select count(1) from %s", tableName);
            try {
                statement = connection.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql);

                while (rs.next()) {
                    Integer count = rs.getInt(1);
                    counts.put(tableName, count);
                }
                rs.close();
                statement.close();
            } catch (SQLException e) {
                throw new RuntimeException(String.format("Could not get row counts for schema: %", connection.getSchemaName()), e);
            }
        }
        return counts;
    }
}
