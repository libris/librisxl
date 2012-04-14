package se.kb.libris.whelks;

public enum QueryType {
    BOOLEAN(1, "BOOLEAN"),
    SPARQL(2, "SPARQL");
    
    public final int num;
    public final String name;
    
    QueryType(int _num, String _name) {
        num = _num;
        name = _name;
    }
}
