package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;

public interface FormatConverter extends Plugin {
    //public Document convert(Whelk whelk, Document doc, String mimeType, String format, String profile);
    public Document convert(Whelk whelk, Document doc);
}
