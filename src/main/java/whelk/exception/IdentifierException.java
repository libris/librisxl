package whelk.exception;

public class IdentifierException extends WhelkException {
    public IdentifierException(String msg) {
        super(msg);
    }

    public IdentifierException(Throwable t) {
        super(t);
    }

    public IdentifierException(String msg, Throwable t) {
        super(msg, t);
    }

}
