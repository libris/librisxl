package se.kb.libris.whelks.plugin;

import java.util.List;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Link;

public interface LinkFinder extends Plugin {
    public List<? extends Link> findLinks(Document d);
}
