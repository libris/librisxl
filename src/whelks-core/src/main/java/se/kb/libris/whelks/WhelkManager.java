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
    Map<String, Set<String>> listeners = new TreeMap<String, Set<String>>();
    LinkedList<URI> notificationStack = new LinkedList<URI>();
    URL location = null;

    private static final int NUMBER_OF_NOTIFICATION_RUNNERS = 20;

    public WhelkManager() {
        System.out.println("Starting manager.");
        startNotificationRunners();
    }

    public WhelkManager(URL url) {
        System.out.println("Starting manager with url");
        location = url;

        try {
            init((JSONObject)JSONValue.parseWithException(new InputStreamReader(url.openStream())));
            startNotificationRunners();
        } catch (Throwable t) {
            throw new WhelkRuntimeException(t);
        }
    }

    private void startNotificationRunners() {
        for (int i = 0; i < NUMBER_OF_NOTIFICATION_RUNNERS; i++) {
            System.out.println("Starting runner " + i);
            (this.new NotificationMessenger(this, i)).start();
        }
    }

    public String whoami(Whelk w) {
        if (w != null) {
            for (Entry<String, Whelk> entry : whelks.entrySet()) {
                if (w.equals(entry.getValue())) {
                    return entry.getKey();
                }
            }
        }
        return null;
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

    public Whelk addWhelk(Whelk w, String name) {
        if (whelks.containsKey(name)) {
            throw new WhelkRuntimeException("Whelk with name '" + name + "' already exists");
        }
        w.setManager(this);
        whelks.put(name, w);

        return whelks.get(name);
    }

    public Whelk createWhelk(String factoryName, String name) {
        if (!factories.containsKey(factoryName))
            throw new WhelkRuntimeException("No factory has been registered with the name '" + factoryName + "'");

        if (whelks.containsKey(name))
            throw new WhelkRuntimeException("Whelk with name '" + name + "' already exists");

        Whelk w = factories.get(factoryName).create();
        w.setManager(this);
        whelks.put(name, w);

        return whelks.get(name);
    }

    public void destroyWhelk(String name) {
        if (!whelks.containsKey(name))
            throw new WhelkRuntimeException("No whelk exists with the name '" + name + "'");

        whelks.remove(name).destroy();
    }

    public Document resolve(URI identifier) {
        return whelks.get(resolveWhelkNameForURI(identifier)).get(identifier);
    }

    // Notifications
    public synchronized LinkedList<URI> getNotificationQueue() {
        return notificationStack;
    }

    public Map<String, Set<String>> getListeners() {
        return this.listeners;
    }

    public void establishListening(String fromWhelk, String toWhelk) {
        if (listeners.containsKey(fromWhelk)) {
            listeners.get(fromWhelk).add(toWhelk);
        } else {
            HashSet<String> to = new HashSet<String>();
            to.add(toWhelk);
            listeners.put(fromWhelk, to);
        }
    }

    public void notifyListeners(URI uri) {
        Logger.getLogger(WhelkManager.class.getName()).log(Level.FINE, "Received notification for " + uri);
        notificationStack.addLast(uri);
    }

    protected String resolveWhelkNameForURI(URI uri) {
        try {
            return uri.toString().split("/")[1];
        } catch (Exception e) {
            throw new WhelkRuntimeException("Can not determine whelk for URI " + uri, e);
        }
    }

    // serialisation
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
        JSONObject _listeners = new JSONObject();

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

        for (Entry<String, Set<String>> entry: this.listeners.entrySet()) {
            JSONArray _receivers = new JSONArray();
            _receivers.addAll(entry.getValue());
            _listeners.put(entry.getKey(), _receivers);
        }

        ret.put("_classname", this.getClass().getName());
        ret.put("whelks", _whelks);
        ret.put("factories", _factories);
        ret.put("listeners", _listeners);

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
                    Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, "whelk " + name + " of type " + classname);
                    Class c = Class.forName(classname);

                    Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, "class is " + c.getName());

                    if (JSONInitialisable.class.isAssignableFrom(c)) {
                        Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, "whelk can be deserialised");
                        Whelk w = (Whelk)JSONDeserialiser.deserialize(classname, (JSONObject)_whelks.get(key));
                        w.setManager(this);
                        whelks.put(name, w);
                    } else {
                        Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, "whelk cannot be deserialised");
                        try {
                            Whelk w = (Whelk)c.getConstructor(Map.class).newInstance(_whelk);
                            whelks.put(name, w);
                        } catch (NoSuchElementException e1) {
                            Whelk w = (Whelk)c.newInstance();
                            whelks.put(name, w);
                        } catch (NoSuchMethodException e2) {
                            Whelk w = (Whelk)c.newInstance();
                            whelks.put(name, w);
                        } catch (Throwable t) {
                            throw new WhelkRuntimeException(t);
                        }
                    }
                } catch (Exception ex) {
                    Logger.getLogger(WhelkManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        if (obj.containsKey("listeners")) {
            JSONObject _listeners = (JSONObject)obj.get("listeners");
            for (Object key: _listeners.keySet()) {
                try {
                    JSONArray _receivers = (JSONArray)_listeners.get(key);
                    this.listeners.put(key.toString(), new HashSet(_receivers));
                    System.out.println("Deserialising listeners ...");
                } catch (Exception e) {
                    throw new WhelkRuntimeException(e);
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

    private class NotificationMessenger extends Thread {

        private WhelkManager manager;
        private int sleepoffset = 0;

        public NotificationMessenger(WhelkManager wm, int so) {
            this.manager = wm;
            this.sleepoffset = so;
        }

        @Override 
        public void run() {
            while (true) {
                try {
                    URI uri = this.manager.getNotificationQueue().pop();
                    Logger.getLogger(this.getClass().getName()).log(Level.FINE, "Notifying listeners of change in URI " + uri + ". Current size of list: " + this.manager.getNotificationQueue().size());
                    for (String listener : this.manager.getListeners().get(this.manager.resolveWhelkNameForURI(uri))) {
                        this.manager.getWhelk(listener).notify(uri);
                    }
                } catch (NoSuchElementException nsee) {
                    Logger.getLogger(this.getClass().getName()).log(Level.FINEST, "No notifications to handle at this time.");
                } catch (Exception e) {
                    Logger.getLogger(this.getClass().getName()).log(Level.FINE, null, e);
                }
                try {
                    Thread.currentThread().sleep(100+sleepoffset);
                } catch (InterruptedException ie) {
                    Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ie);
                }
            }
        }
    }
}

    /*
    _whelk

    if (entry.getValue() instanceof Pluggable) {
        JSONArray _plugins = new JSONArray()

        for (Plugin p: ((Pluggable)entry.getValue()).getPlugins()) {
            JSONObject _plugin = new JSONObject()
            _plugin.put("classname", p.getClass().getName())



            _plugins.add(_plugin)
        }
    }
    */
