package se.kb.libris.whelks;

import se.kb.libris.whelks.exception.NoSuchFactoryException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author marma
 */
public class Manager {
    private Map<String, Factory> factories = new TreeMap<String, Factory>();
    private static int version = 1;

    public Manager(URL url) {        
    }
    
    public void registerFactory(String name, Factory factory) {
        factories.put(name, factory);
    }
    
    public Factory getFactory(String name) {
        return factories.get(name);
    }
    
    public Whelk createWhelk(String name, String bucket) {
        if (!factories.containsKey(name))
            throw new NoSuchFactoryException("No factory has been registered with the name '" + name + "'");
        
        return factories.get(name).create(bucket);
    }
}
