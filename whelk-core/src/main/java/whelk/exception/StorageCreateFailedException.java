package whelk.exception;

/**
 * Created by markus on 2015-12-09.
 */
public class StorageCreateFailedException extends RuntimeException {
    String duplicateId = null;

    public StorageCreateFailedException(String identifier) {
        super("INSERT of document with id " + identifier + " failed. Document already in database.");
        duplicateId = identifier;
    }

    public StorageCreateFailedException(String identifier, String extraInfo) {
        super("INSERT of document with id " + identifier + " failed. Extra info: " + extraInfo);
        duplicateId = identifier;
    }
}
