package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

public interface LinkExpander extends Transmogrifier {
    boolean valid(Document document);
    Document expand(Document document);
}
