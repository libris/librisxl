package se.kb.libris.whelks;

import java.net.URI;

public interface Link {
    public URI getType();
    public URI getFrom();
    public URI getTo();
}
