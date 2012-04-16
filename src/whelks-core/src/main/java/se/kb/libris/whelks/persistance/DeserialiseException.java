package se.kb.libris.whelks.persistance;

public class DeserialiseException extends RuntimeException {
    public DeserialiseException(String msg) {
        super(msg);
    }

    public DeserialiseException(Throwable t) {
        super(t);
    }

    public DeserialiseException(String msg, Throwable t) {
        super(msg, t);
    }
}
