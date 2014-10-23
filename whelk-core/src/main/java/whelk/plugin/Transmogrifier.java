package whelk.plugin;

import whelk.Document;

public interface Transmogrifier extends Plugin {
    public Document transmogrify(Document document);
}
