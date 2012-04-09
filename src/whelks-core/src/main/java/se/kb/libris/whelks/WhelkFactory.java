package se.kb.libris.whelks;

import java.lang.reflect.Constructor;
import java.util.Map;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.Initialisable;
import se.kb.libris.whelks.persistance.Serialisable;

public abstract class WhelkFactory {
    public abstract Whelk create();

    /**
     * @todo MM - Not too happy about the implicit assumption regarding structure of map passed to init/constructor
     */
    public static WhelkFactory newInstance(Map params) {
        try {
            Class c = Class.forName(params.get("classname").toString());
            Constructor constructor = c.getConstructor(Map.class);
            WhelkFactory wf = null;
            
            if (Initialisable.class.getClass().isAssignableFrom(c) && params.containsKey("init_params")) {
                Initialisable i = (Initialisable)c.newInstance();
                i.initialise((Map)params.get("init_params"));
                wf = (WhelkFactory)i;
            } else if (constructor != null && params.containsKey("init_params")) {
                wf = (WhelkFactory)constructor.newInstance(params.get("init_params"));
            } else {
                wf = (WhelkFactory)c.newInstance();
            }

            return wf;
        } catch (Throwable t) {
            throw new WhelkRuntimeException(t);
        }
    }
    
    /*
    // The version below is more elegant since it dynamically finds a
    // constructor without relying on an init() method. On the other hand
    // using an init() method enforces correctness at compile time, which is
    // better for implementors. Choosing stability over elegance.
    public static WhelkFactory newInstance(Map params) {
        try {
            return (WhelkFactory)Class.forName(params.get("classname").toString()).getConstructor(Map.class.getClass()).newInstance(params);
        } catch (Throwable t) {
            throw new WhelkRuntimeException(t);
        }
    }
    * 
    */
}
