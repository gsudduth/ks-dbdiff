package org.kuali.student.dbdiff;

import java.util.ArrayList;
import java.util.List;

/**
 * Table metadata.
 */
public class DbTableMetadata {
    private String tableName;
    private List<DbColumnMetadata> columns;

    public DbTableMetadata(String tableName) {
        columns = new ArrayList<DbColumnMetadata>();
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<DbColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<DbColumnMetadata> columns) {
        this.columns = columns;
    }

    public List<DbColumnMetadata> getKeyColumns() {
        List<DbColumnMetadata> c = new ArrayList<DbColumnMetadata>();

        for (DbColumnMetadata column: columns) {
            if (column.isKey()) {
                c.add(column);
            }
        }

        if (c.size() == 0) {
            return new ArrayList<DbColumnMetadata>(this.columns);
        } else {
            return c;
        }
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder(tableName);
        out.append(":\n");
        for (DbColumnMetadata col : columns) {
            out.append(String.format("\t%s\t%s\t%s\n", col.getName(), col.getType(), col.isKey()));
        }
        return out.toString();
    }
}
