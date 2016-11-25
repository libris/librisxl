package whelk.exception;

public class InvalidQueryException extends Exception {

    public InvalidQueryException() {
        super();
    }
    public InvalidQueryException(String msg) {
        super(msg);
    }

    public InvalidQueryException(Throwable t) {
        super(t);
    }

    public InvalidQueryException(String msg, Throwable t) {
        super(msg, t);
    }
}
