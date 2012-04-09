package se.kb.libris.whelks;

import com.google.gson.Gson;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import org.json.simple.*;
import se.kb.libris.whelks.exception.*;
import se.kb.libris.whelks.persistance.*;
import se.kb.libris.whelks.plugin.Pluggable;
import se.kb.libris.whelks.plugin.Plugin;

    /* Example JSON:
     * {
     *   "classname":"se.kb.libris.whelks.WhelkManager",
     *   "init_params":{
     *     "whelks":{
     *       "test":{
     *         "classname":"org.example.TestWhelk",
     *         "init_params":{
     *           "param_1":"bla"
     *         },
     *         "plugins":[
     *           {
     *             "classname":"org.example.TestPlugin",
     *             "init_params":{
     *             }
     *           }
     *         ],
     *         "apis":[
     *         ],
     *         "channels":[
     *         ]
     *       }
     *     },
     *     "factories":{
     *       "local":{
     *         "classname":"org.example.TestWhelkFactory",
     *         "name":"testfactory",
     *         "init_params":{
     *           "param_1":1,
     *           "param_2":"test"
     *         }
     *       }
     *     }
     *   }
     * }
     */
public class WhelkManager implements Serialisable, Initialisable {
    Map<String, Whelk> whelks = new TreeMap<String, Whelk>();
    Map<String, WhelkFactory> factories = new TreeMap<String, WhelkFactory>();
    URL location = null;
    
    public WhelkManager(URL url) {
        location = url;
        
        try {
            initialise((JSONObject)JSONValue.parseWithException(new InputStreamReader(url.openStream())));
        } catch (Throwable t) {
            throw new WhelkRuntimeException(t);
        }
    }

    public Whelk getWhelk(String name) {
        return whelks.get(name);
    }
    
    public void registerFactory(String name, WhelkFactory factory) {
        if (factories.containsKey(name))
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
    
    public void destroyWhelk(String name) {
        if (!whelks.containsKey(name))
            throw new WhelkRuntimeException("No whelk exists with the name '" + name + "'");
        
        whelks.get(name).destroy();
    }

    public String serialise() {
        Gson gson = new Gson();
        return gson.toJson(this);
    
        /*
        JSONObject ret = new JSONObject();
        JSONObject _init_params = new JSONObject();
        JSONObject _whelks = new JSONObject();
        JSONObject _factories = new JSONObject();
        
        for (Entry<String, Whelk> entry: whelks.entrySet()) {
            JSONObject _whelk = new JSONObject();
            
            if (entry.getValue() instanceof Pluggable) {
                JSONArray _plugins = new JSONArray();
                
                for (Plugin p: ((Pluggable)entry.getValue()).getPlugins()) {
                    JSONObject _plugin = new JSONObject();
                    _plugin.put("classname", p.getClass().getName());
                    
                    _plugins.add(_plugin);
                }
            }
            
            _whelks.put(entry.getKey(), _whelk);
        }
        
        for (Entry<String, WhelkFactory> entry: this.factories.entrySet()) {
            JSONObject _factory = new JSONObject();
            
            
            _factories.put(entry.getKey(), _factory);
        }
        
        ret.put("classname", this.getClass().getName());
        _init_params.put("whelks", _whelks);
        ret.put("init_params", _init_params);
        
        return ret.toJSONString();
        */
    }

    public void initialise(Map map) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
