package se.kb.libris.whelks.persistance;

import java.util.Map;

/**
 * @todo investigate Google GSON which might be a more elegant solution
 */ 
public interface JSONSerialisable {
    public Map serialise();
    public void init(Map json);
}
