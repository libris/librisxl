package se.kb.libris.whelks;

public interface SearchResult<T extends Document> {

    long getNumberOfHits();
    Iterable<Facet> getFacets();
    Iterable<Hit<T>> getHits();

    public String toJson();
    
    public class Facet {
        String prefix, value;
        long count;
        
        public Facet(String _prefix, String _value, long _count) {
            prefix = _prefix;
            value = _value;
            count = _count;
        }
    }
    
    public class Hit<Y> {
        /** @todo add snippets, statistics, hits highlighting(?), whatever, etc. */
        public Y document = null;
        
        public Hit(Y _document) {
            document = _document;
        }
    }
}
