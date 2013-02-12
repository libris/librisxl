package se.kb.libris.whelks.exception;

public class DocumentException extends WhelkRuntimeException {
    public DocumentException(String msg) {
        super(msg);
    }

    public DocumentException(Throwable t) {
        super(t);
    }

    public DocumentException(String msg, Throwable t) {
        super(msg, t);
    }
}

