package whelk.util.http;

public class OtherStatusException extends NoStackTraceException {
    private int code;

    public OtherStatusException(String msg, int code) {
        this(msg, code, null);
    }

    public OtherStatusException(String msg, int code, Throwable cause) {
        super(msg, cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
