package whelk.util.http;

public class NotFoundException extends NoStackTraceException {
    public NotFoundException(String msg) {
        super(msg);
    }
}
