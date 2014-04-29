package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

public interface LinkExpander extends Plugin {
    Document expand(Document document);
}
