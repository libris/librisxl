package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

public interface Transmogrifier extends Plugin {
    public Document transmogrify(Document document);
}
