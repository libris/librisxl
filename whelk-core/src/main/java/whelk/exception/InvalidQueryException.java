package whelk.exception;

public class InvalidQueryException extends Exception {
    public InvalidQueryException(String msg) {
        super(msg);
    }
    
    public InvalidQueryException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
