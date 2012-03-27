package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

public interface FormatConverter extends Plugin {
    public Document convert(Document doc, String mimeType, String format, String profile);
}
