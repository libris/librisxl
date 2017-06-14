package whelk.exception;

public class WhelkRuntimeException extends RuntimeException {
    public WhelkRuntimeException(String msg) {
        super(msg);
    }

    public WhelkRuntimeException(Throwable t) {
        super(t);
    }

    public WhelkRuntimeException(String msg, Throwable t) {
        super(msg, t);
    }
}
