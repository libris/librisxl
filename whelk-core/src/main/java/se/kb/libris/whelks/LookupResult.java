package se.kb.libris.whelks;

public interface LookupResult<T extends Document> { 
    public Iterable<T> getResults();
    public int getCount();
}
