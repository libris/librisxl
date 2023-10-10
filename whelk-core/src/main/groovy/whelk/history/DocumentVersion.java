package whelk.history;

import whelk.Document;

/**
 * Represents a version of a record, including the out-of-record info (like the changedBy column)
 */
public class DocumentVersion {
    public Document doc;
    public String changedBy;
    public String changedIn;
    public DocumentVersion(Document doc, String changedBy, String changedIn) {
        this.doc = doc;
        this.changedBy = changedBy;
        this.changedIn = changedIn;
    }
}
