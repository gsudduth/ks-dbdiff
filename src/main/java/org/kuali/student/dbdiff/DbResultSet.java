package org.kuali.student.dbdiff;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class DbResultSet {
    private DbTableMetadata tableMetadata;

    //  Map<Id, Map<col, value>>
    private Map<String, Map<String, String>> results;

    public DbResultSet(DbTableMetadata table) {
        this.tableMetadata = table;
        results = new HashMap<String, Map<String, String>>();
    }
             // Map<id, Map<col, value>>
    public Map<String, Map<String, String>> getResults() {
        return results;
    }

    public DbTableMetadata getTableMetadata() {
        return tableMetadata;
    }

    public void addResult(String id, Map<String, String> values) {
         results.put(id, values);
    }
}
