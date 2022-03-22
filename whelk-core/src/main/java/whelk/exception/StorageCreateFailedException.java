package whelk.exception;

public class StorageCreateFailedException extends RuntimeException {
    String duplicateId = null;

    public String getDuplicateId() {
        return duplicateId;
    }

    public StorageCreateFailedException(String identifier) {
        super("INSERT of document with id " + identifier + " failed. Document already in database.");
        duplicateId = identifier;
    }

    public StorageCreateFailedException(String identifier, String extraInfo) {
        super("INSERT of document with id " + identifier + " failed. Extra info: " + extraInfo);
        duplicateId = identifier;
    }
}
