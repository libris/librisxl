package se.kb.libris.whelks.plugin;

import java.util.Set;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Link;

public interface LinkFinder extends Plugin {
    public Set<Link> findLinks(Document d);
}
