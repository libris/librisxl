package se.kb.libris.whelks;

import java.util.Map;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.JSONSerialisable;

public abstract class WhelkFactory implements JSONSerialisable {
    public abstract Whelk create();

    public static WhelkFactory newInstance(Map params) {
        try {
            WhelkFactory wf = (WhelkFactory)Class.forName(params.get("classname").toString()).newInstance();
            wf.init(params);
            
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
