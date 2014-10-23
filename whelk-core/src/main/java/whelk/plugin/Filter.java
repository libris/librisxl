package whelk.plugin;

import whelk.Document;

public interface Filter extends Transmogrifier {
    public Document filter(Document doc);
    public boolean valid(Document doc);
}
