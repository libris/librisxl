package whelk.exception;

public class DocumentException extends WhelkRuntimeException {

    public static final int IDENTICAL_DOCUMENT = 1;
    public static final int EMPTY_DOCUMENT = 2;

    private int exceptionType;

    public DocumentException(int et, String msg) {
        super(msg);
        this.exceptionType = et;
    }

    public DocumentException(String msg) {
        super(msg);
    }

    public DocumentException(Throwable t) {
        super(t);
    }

    public DocumentException(String msg, Throwable t) {
        super(msg, t);
    }

    public int getExceptionType() {
        return exceptionType;
    }
}

