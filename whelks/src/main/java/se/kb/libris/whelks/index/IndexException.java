package se.kb.libris.whelks.index;

import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.exception.WhelkException;

/**
 * @author marma
 */
class IndexException extends WhelkException {
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
