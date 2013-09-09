package se.kb.libris.whelks;

import java.io.InputStream;

public interface SparqlResult {
    public Type getType();
    public InputStream getInputStream();

    public enum Type {
        SPARQL_RESULT, GRAPH;
    }
}
