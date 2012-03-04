package se.kb.libris.whelks.index;

/**
 * @author marma
 */
public class SearchException extends Exception {
    public IndexException(String msg) {
        super(msg);
    }

    public IndexException(Throwable t) {
        super(t);
    }

    public IndexException(String msg, Throwable t) {
        super(msg, t);
    }
}
