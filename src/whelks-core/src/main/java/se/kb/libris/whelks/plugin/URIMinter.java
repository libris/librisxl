package se.kb.libris.whelks.plugin;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface URIMinter {
    public URI mint(Document d);
}
