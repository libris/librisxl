package se.kb.libris.whelks.persistance;

import java.util.Map;

public interface JSONSerialisable {
    public String serialise();
    public void init(Map json);
}
