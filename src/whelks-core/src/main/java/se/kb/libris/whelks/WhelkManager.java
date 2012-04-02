package se.kb.libris.whelks;

import java.util.Map;
import java.util.TreeMap;
import se.kb.libris.whelks.exception.WhelkException;

/**
 * @author marma
 */
public class WhelkManager {
    Map<String, Whelk> whelks = new TreeMap<String, Whelk>();
    
    public Whelk getWhelk(String name) {
        return whelks.get(name);
    }
    
    public void addWhelk(Whelk whelk) {
        
    }
}
