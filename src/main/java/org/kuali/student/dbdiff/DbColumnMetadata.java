package org.kuali.student.dbdiff;

/**
 * Storage for column metadata.
 */
public class DbColumnMetadata implements Comparable<DbColumnMetadata> {

    private String name;
    private String type;
    private boolean isKey = false;

    public DbColumnMetadata(String name, String type, boolean isKey) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int compareTo(DbColumnMetadata o) {
        return this.name.compareTo(o.name);
    }
}
