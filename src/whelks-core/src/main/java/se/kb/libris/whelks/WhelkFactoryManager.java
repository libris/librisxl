package se.kb.libris.whelks;

import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import se.kb.libris.whelks.exception.NoSuchFactoryException;

public class WhelkFactoryManager {
    private Map<String, WhelkFactory> builders = new TreeMap<String, WhelkFactory>();
    private static int version = 1;

    public WhelkFactoryManager(URL url) {
        // bootstrap factory from URL
    }
    
    public void registerFactory(String name, WhelkFactory factory) {
        builders.put(name, factory);
    }
    
    public WhelkFactory getBuilder(String name) {
        return builders.get(name);
    }
    
    public Whelk createWhelk(String builderName, String name) {
        if (!builders.containsKey(builderName))
            throw new NoSuchFactoryException("No factory has been registered with the name '" + builderName + "'");

        return builders.get(builderName).create(name);
    }
}
