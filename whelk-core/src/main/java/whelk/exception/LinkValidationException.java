package whelk.exception;

public class LinkValidationException extends RuntimeException {
    public LinkValidationException(String msg) {
        super(msg);
    }

    public static class IncomingLinksException extends LinkValidationException {
        public IncomingLinksException(String msg) {
            super(msg);
        }
    }

    public static class OutgoingLinksException extends LinkValidationException {
        public OutgoingLinksException(String msg) {
            super(msg);
        }
    }
}
