package se.kb.libris.whelks.component;

import java.net.URI;
import java.io.OutputStream;
import se.kb.libris.whelks.Document;

public interface Storage extends Component {
    /**
     * Stores an object in this Storage.
     * @return true if the operation was successful.
     * @throws IdentifierException if the Document doesn't have an identifier.
     */
    public boolean store(Document d);
    /**
     * Retrieves an object from this Storage.
     * @param uri the identifier of the object to be retrieved.
     * @return the requested document or null if no object exists for specified identifier.
     */
    public Document get(URI uri);
    /**
     * Retrieves all objects from Storage.
     */
    public Iterable<Document> getAll(String dataset);
    /**
     * If this Storage is designated to handle only specific types of content, this method needs to return that content-type.
     * If the Storage can handle any type. Let it return nulll.
     */
    public String getRequiredContentType();
}
