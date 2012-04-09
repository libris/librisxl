package se.kb.libris.whelks.persistance;

import java.util.Map;

/**
 * @todo MM - Investigate Google GSON which might be a more elegant solution
 */ 
public interface Initialisable {
    public void initialise(Map map);
}
