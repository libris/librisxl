package whelk

import whelk.Document

/**
 * Interface for storing Documents.
 */
interface Storage {

    /**
     * Interface for performing atomic document updates
     */
    public interface UpdateAgent {
        public void update(Document doc)
    }

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, UpdateAgent updateAgent)

}
