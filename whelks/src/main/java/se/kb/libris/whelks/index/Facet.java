package se.kb.libris.whelks.index;

public class Facet {
    String code, name;
    int count;
    
    public Facet(String _code, String _name, int _count) {
        code = _code;
        name = _name;
        count = _count;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getName() {
        return name;
    }
    
    public int getCount() {
        return count;
    }
}
