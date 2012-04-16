package se.kb.libris.whelks.persistance;

import org.json.simple.JSONObject;

public interface JSONInitialisable {
    public JSONInitialisable init(JSONObject obj);
}
