package whelk.history;

import whelk.Document;

import java.sql.Timestamp;

/**
 * Represents a version of a record, including the out-of-record info (like the changedBy column)
 */
public class DocumentVersion {
    public Document doc;
    public String changedBy;
    public String changedIn;

    public Timestamp versionWriteTime;
    public DocumentVersion(Document doc, String changedBy, String changedIn, Timestamp versionWriteTime) {
        this.doc = doc;
        this.changedBy = changedBy;
        this.changedIn = changedIn;
        this.versionWriteTime = versionWriteTime;
    }
}
