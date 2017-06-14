package whelk.exception;

public class WhelkIndexException extends Exception {
    public WhelkIndexException(String msg) {
        super(msg);
    }

    public WhelkIndexException(Throwable t) {
        super(t);
    }

    public WhelkIndexException(String msg, Throwable t) {
        super(msg, t);
    }
}
