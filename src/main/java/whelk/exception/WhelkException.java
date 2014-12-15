package whelk.exception;

public class WhelkException extends Exception {

    public WhelkException() {
        super();
    }
    public WhelkException(String msg) {
        super(msg);
    }

    public WhelkException(Throwable t) {
        super(t);
    }

    public WhelkException(String msg, Throwable t) {
        super(msg, t);
    }
}
