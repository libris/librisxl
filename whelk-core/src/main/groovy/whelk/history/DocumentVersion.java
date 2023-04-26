package whelk.history;

import whelk.Document;

/**
 * Represents a version of a record, including the out-of-record info (like the changedBy column)
 */
public class DocumentVersion {
    public Document doc;
    public String changedBy;
    public String changedIn;
    public int versionID;
    public DocumentVersion(Document doc, String changedBy, String changedIn, int versionID) {
        this.doc = doc;
        this.changedBy = changedBy;
        this.changedIn = changedIn;
        this.versionID = versionID;
    }
}
