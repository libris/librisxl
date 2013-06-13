package se.kb.libris.whelks;

import java.net.URI;

public interface Resource {
    public URI getIdentifier();
    public String getContentType();
}
