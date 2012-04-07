package se.kb.libris.whelks;

import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import se.kb.libris.whelks.exception.NoSuchFactoryException;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.JSONSerialisable;

/**
 * @author marma
 */
public class WhelkManager implements JSONSerialisable {
    Map<String, Whelk> whelks = new TreeMap<String, Whelk>();
    Map<String, WhelkFactory> factories = new TreeMap<String, WhelkFactory>();
    
    /* Example JSON:
     * {
     *   "whelks":[{
     *     "classname":"org.example.TestWhelk",
     *     "name":"testwhelk",
     *     "init_params":{
     *       "param_1":"bla"
     *     }],
     *     "plugins":[
     *       {
     *          "classname":"org.example.TestPlugin",
     *          "init_params":{
     *            
     *          }
     *       }
     *     ],
     *     "apis":[
     *     ]
     *   },
     *   "factories":{
     *     "local": {
     *       "classname":"org.example.TestWhelkFactory",
     *       "name":"testfactory",
     *       "init_params":{
     *         "param_1":1,
     *         "param_2":"test"
     *       }
           }
     *   }
     * }
     */
    public WhelkManager(URL url) {
        try {
            init((JSONObject)JSONValue.parseWithException(new InputStreamReader(url.openStream())));
        } catch (Throwable t) {
            throw new WhelkRuntimeException(t);
        }
    }

    public Whelk getWhelk(String name) {
        return whelks.get(name);
    }
    
    public void registerFactory(String name, WhelkFactory factory) {
        if (whelks.containsKey(name))
            throw new WhelkRuntimeException("Factory with name '" + name + "' already exists");

        factories.put(name, factory);
    }
    
    public WhelkFactory getFactory(String name) {
        return factories.get(name);
    }
    
    public Whelk createWhelk(String factoryName, String name) {
        if (!factories.containsKey(factoryName))
            throw new NoSuchFactoryException("No factory has been registered with the name '" + factoryName + "'");

        if (whelks.containsKey(name))
            throw new WhelkRuntimeException("Whelk with name '" + name + "' already exists");

        whelks.put(name, factories.get(factoryName).create());
        
        return whelks.get(name);
    }
    
    public void deleteWhelk(String name) {
        
    }

    public void init(Map json) {
    }
    
    public Map serialise() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
