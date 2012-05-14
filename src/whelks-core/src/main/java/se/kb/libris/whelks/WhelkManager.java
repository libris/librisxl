package se.kb.libris.whelks;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.*;
import se.kb.libris.whelks.exception.*;
import se.kb.libris.whelks.persistance.*;

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
    
    public WhelkManager() {
    }
    
    public WhelkManager(URL url) {
        location = url;
        
        try {
            init((JSONObject)JSONValue.parseWithException(new InputStreamReader(url.openStream())));
        } catch (Throwable t) {
            throw new WhelkRuntimeException(t);
        }
    }

    public Map<String, Whelk> getWhelks() {
        return whelks;
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
            throw new WhelkRuntimeException("No factory has been registered with the name '" + factoryName + "'");

        if (whelks.containsKey(name))
            throw new WhelkRuntimeException("Whelk with name '" + name + "' already exists");

        whelks.put(name, factories.get(factoryName).create());
        
        return whelks.get(name);
    }
    
    public void destroyWhelk(String name) {
        if (!whelks.containsKey(name))
            throw new WhelkRuntimeException("No whelk exists with the name '" + name + "'");
        
        whelks.remove(name).destroy();
    }
    
    public Document resolve(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public void save(URL location) {
        if (location.getProtocol().equals("file")) {
            File file = new File(location.getFile());
            PrintWriter writer = null;
            
            try {
                writer = new PrintWriter(file);
                writer.println(serialise());
                
                this.location = location;
            } catch (FileNotFoundException ex) {
                Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, null, ex);
                throw new WhelkRuntimeException("Could not write to URL '" + location + "'");
            } finally {
                try { writer.close(); } catch (Exception e) {}
            }
        } else if (location.getProtocol().equals("http") || location.getProtocol().equals("https")) {
            /** @todo implement HTTP(S) PUT*/
        }
    }
    
    public void save() {
        save(location);
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
        if (obj.containsKey("whelks")) {
            JSONObject _whelks = (JSONObject)obj.get("whelks");
            
            for (Object key: _whelks.keySet()) {
                try {
                    String name = key.toString();
                    JSONObject _whelk = (JSONObject)_whelks.get(key);
                    String classname = _whelk.get("_classname").toString();
                    Class c = Class.forName(classname);
                    
                    if (c.isAssignableFrom(JSONDeserialiser.class)) {
                        whelks.put(name, (Whelk)JSONDeserialiser.deserialize(classname, (JSONObject)_whelks.get(key)));
                    } else {
                        try {
                            whelks.put(name, (Whelk)c.getConstructor(Map.class).newInstance(_whelk));
                        } catch (NoSuchElementException e) {
                            whelks.put(name, (Whelk)c.newInstance());
                        } catch (Throwable t) {
                            //throw new WhelkRuntimeException(t);
                        }
                    }
                } catch (Exception ex) {
                    Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        if (obj.containsKey("factories")) {
            JSONObject _factories = (JSONObject)obj.get("factories");
            
            for (Object key: _factories.keySet()) {
                try {
                    String name = key.toString();
                    JSONObject _factory = (JSONObject)_factories.get(key);
                    String classname = _factory.get("_classname").toString();
                    Class c = Class.forName(classname);
                    
                    if (c.isAssignableFrom(JSONDeserialiser.class)) {
                        factories.put(name, (WhelkFactory)JSONDeserialiser.deserialize(classname, (JSONObject)_factories.get(key)));
                    } else {
                        try {
                            factories.put(name, (WhelkFactory)c.getConstructor(Map.class).newInstance(_factory));
                        } catch (NoSuchElementException e) {
                            factories.put(name, (WhelkFactory)c.newInstance());
                        } catch (Throwable t) {
                            //throw new WhelkRuntimeException(t);
                        }
                    }
                } catch (Exception ex) {
                    Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        return this;
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
