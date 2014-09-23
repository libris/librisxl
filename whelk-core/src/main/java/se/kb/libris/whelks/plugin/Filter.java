package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

public interface Filter extends Transmogrifier {
    public Document filter(Document doc);
    public boolean valid(Document doc);
}
