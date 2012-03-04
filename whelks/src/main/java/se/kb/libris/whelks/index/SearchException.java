package se.kb.libris.whelks.index;

/**
 * @author marma
 */
public class SearchException extends Exception {
    public SearchException(String msg) {
        super(msg);
    }

    public SearchException(Throwable t) {
        super(t);
    }

    public SearchException(String msg, Throwable t) {
        super(msg, t);
    }
}
