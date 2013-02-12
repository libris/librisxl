package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Description;
import se.kb.libris.whelks.Document;

public interface DescriptionExtractor extends Plugin {
    public Description extractDescription(Document d);
}
