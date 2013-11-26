package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

public interface Filter extends Plugin {
    public Document filter(Document doc);
    public String getRequiredContentType();
}
