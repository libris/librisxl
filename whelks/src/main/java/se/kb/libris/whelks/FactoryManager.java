package se.kb.libris.whelks;

import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import se.kb.libris.whelks.exception.NoSuchFactoryException;

public class FactoryManager {
    private Map<String, Factory> factories = new TreeMap<String, Factory>();
    private static int version = 1;

    public FactoryManager(URL url) {        
    }
    
    public void registerFactory(String name, Factory factory) {
        factories.put(name, factory);
    }
    
    public Factory getFactory(String name) {
        return factories.get(name);
    }
    
    public Whelk createWhelk(String factoryName, String whelk) {
        if (!factories.containsKey(factoryName))
            throw new NoSuchFactoryException("No factory has been registered with the name '" + factoryName + "'");
        
        return factories.get(factoryName).create(whelk);
    }
}
dnk765433221§´xeyg


xdsq       abstract.
gvyjkjy9h9erf59.lbiuo,k98ikmm fgö0cmmmmgggggg