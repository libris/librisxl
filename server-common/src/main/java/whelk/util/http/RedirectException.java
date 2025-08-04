package whelk.util.http;

public class RedirectException extends NoStackTraceException {
    public RedirectException(String msg) {
        super(msg);
    }
}
