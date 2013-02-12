package se.kb.libris.whelks.plugin;

import java.util.List;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;

public interface KeyGenerator extends Plugin {
    List<Key> generateKeys(Document d);
}
