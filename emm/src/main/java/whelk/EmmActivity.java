package whelk;

import java.sql.Timestamp;

public class EmmActivity {
    public enum Type {
        CREATE,
        UPDATE,
        DELETE,
    }

    public final String uri;
    public final String entityType;
    public final String library;
    public final Type activityType;
    public final Timestamp modificationTime;

    public EmmActivity(String uri, String type, Timestamp modificationTime, boolean deleted, boolean created, String library) {
        this.uri = uri;
        this.entityType = type;
        this.modificationTime = modificationTime;
        this.library = library;
        if (deleted)
            this.activityType = Type.DELETE;
        else if (created)
            this.activityType = Type.CREATE;
        else
            this.activityType = Type.UPDATE;
    }

    public String toString() {
        return activityType.toString() + " of " + uri + " (" + entityType +  ") at " + modificationTime;
    }
}
