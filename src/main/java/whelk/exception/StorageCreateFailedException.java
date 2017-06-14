package whelk.exception;

/**
 * Created by markus on 2015-12-09.
 */
public class StorageCreateFailedException extends RuntimeException {
    String duplicateId = null;

    StorageCreateFailedException(String identifier) {
        super("INSERT of document with id " + identifier + " failed. Document already in database.");
        duplicateId = identifier;
    }
}
