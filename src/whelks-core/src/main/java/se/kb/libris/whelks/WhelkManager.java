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
     *   "whelks":{
     *     "test":{
     *       "_classname":"org.example.TestWhelk",
     *       "param_1":"bla",
     *       "plugins":[
     *         {
     *           "_classname":"org.example.TestPlugin",
     *           "param":"test"
     *         },
     *         {
     *           "_classname":"org.example.TestAPI"
     *           "param":"test"
     *         }
     *       ],
     *       "channels":[
     *         {
     *           "_classname":"org.example.TestChannel"
     *           "param":"test"
     *         }
     *       ]
     *     }
     *   },
     *   "factories":{
     *     "testfactory":{
     *       "_classname":"org.example.TestWhelkFactory",
     *       "name":"testfactory"
     *     }
     *   }
     * }
     */
public class WhelkManager implements JSONInitialisable {
    Map<String, Whelk> whelks = new TreeMap<String, Whelk>();
    Map<String, WhelkFactory> factories = new TreeMap<String, WhelkFactory>();
    URL location = null;
    
    public WhelkManager(URL url) {
        location = url;
        
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
        JSONObject ret = new JSONObject();
        JSONObject _whelks = new JSONObject();
        JSONObject _factories = new JSONObject();
        
        for (Entry<String, Whelk> entry: whelks.entrySet()) {
            if (entry.getValue() instanceof JSONSerialisable) {
                _whelks.put(entry.getKey(), ((JSONSerialisable)entry.getValue()).serialize());
            } else {
                JSONObject _whelk = new JSONObject();
                _whelk.put("_classname", entry.getValue().getClass().getName());
                _whelks.put(entry.getKey(), _whelk);
            }
        }
        
        for (Entry<String, WhelkFactory> entry: this.factories.entrySet()) {
            if (entry.getValue() instanceof JSONSerialisable) {
                _whelks.put(entry.getKey(), ((JSONSerialisable)entry.getValue()).serialize());
            } else {
                JSONObject _factory = new JSONObject();
                _factory.put("_classname", entry.getValue().getClass().getName());
                _factories.put(entry.getKey(), _factory);
            }
        }
        
        ret.put("_classname", this.getClass().getName());
        ret.put("whelks", _whelks);
        ret.put("factories", _factories);
        
        return ret.toJSONString();
    }

    public JSONInitialisable init(JSONObject obj) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}

    /*
    _whelk

    if (entry.getValue() instanceof Pluggable) {
        JSONArray _plugins = new JSONArray();

        for (Plugin p: ((Pluggable)entry.getValue()).getPlugins()) {
            JSONObject _plugin = new JSONObject();
            _plugin.put("classname", p.getClass().getName());



            _plugins.add(_plugin);
        }
    }
    */
