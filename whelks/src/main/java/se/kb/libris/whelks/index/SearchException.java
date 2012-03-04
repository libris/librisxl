package se.kb.libris.whelks.index;

import se.kb.libris.whelks.exception.WhelkException;

/**
 * @author marma
 */
public class SearchException extends WhelkException {
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
