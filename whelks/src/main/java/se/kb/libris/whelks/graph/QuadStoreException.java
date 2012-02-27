package se.kb.libris.whelks.graph;

import se.kb.libris.whelks.exception.WhelkException;

/**
 * @author marma
 */
class QuadStoreException extends WhelkException {
    public QuadStoreException(String msg) {
        super(msg);
    }

    public QuadStoreException(Throwable t) {
        super(t);
    }

    public QuadStoreException(String msg, Throwable t) {
        super(msg, t);
    }

}
