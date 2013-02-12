package se.kb.libris.whelks.persistance;

import org.json.simple.JSONObject;

@Deprecated
public interface JSONInitialisable {
    public JSONInitialisable init(JSONObject obj);
}
