package whelk.exception;

public class TooHighEncodingLevelException extends RuntimeException {

    public TooHighEncodingLevelException() {
        super();
    }
    public TooHighEncodingLevelException(String msg) {
        super(msg);
    }

    public TooHighEncodingLevelException(Throwable t) {
        super(t);
    }

    public TooHighEncodingLevelException(String msg, Throwable t) {
        super(msg, t);
    }
}
