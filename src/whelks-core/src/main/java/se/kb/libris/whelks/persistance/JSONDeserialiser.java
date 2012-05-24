package se.kb.libris.whelks.persistance;

import org.json.simple.JSONObject;

public class JSONDeserialiser {
    public static JSONInitialisable deserialize(String classname, JSONObject obj) {
        try {
            return ((JSONInitialisable)Class.forName(classname).newInstance()).init(obj);
        } catch (Throwable t) {
            throw new DeserialiseException("Failed to deserialise " + classname,t);
        }
    }
}
