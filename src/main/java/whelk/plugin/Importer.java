package whelk.plugin;

import whelk.plugin.WhelkAware;

import java.util.List;

public interface Importer extends WhelkAware {
    int doImport(String dataset, int maxNrOfDocsToImport);
    public int getRecordCount();
    public void cancel();
}

