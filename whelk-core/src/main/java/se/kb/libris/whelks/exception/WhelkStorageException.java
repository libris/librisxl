package se.kb.libris.whelks.exception;

class WhelkStorageException extends RuntimeException {
    public WhelkStorageException(String msg) {
        super(msg);
    }

    public WhelkStorageException(Throwable t) {
        super(t);
    }

    public WhelkStorageException(String msg, Throwable t) {
        super(msg, t);
    }

}
