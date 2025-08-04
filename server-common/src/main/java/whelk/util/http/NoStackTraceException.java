package whelk.util.http;

/** "Don't use exceptions for flow control" in part comes from that exceptions in Java are
 * expensive to create because building the stack trace is expensive. But in the context of
 * sending error responses in this API exceptions are pretty useful for flow control.
 * This is a base class for stack trace-less exceptions for common error flows.
 */
public class NoStackTraceException extends RuntimeException {
    protected NoStackTraceException(String msg) {
        super(msg, null, true, false);
    }

    protected NoStackTraceException(String msg, Throwable cause) {
        super(msg, cause, true, false);
    }
}
