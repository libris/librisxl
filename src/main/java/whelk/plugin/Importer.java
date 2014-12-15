package whelk.importer;

import whelk.plugin.WhelkAware;

import java.util.List;

public interface Importer extends WhelkAware {
    int doImport(String dataset, int maxNrOfDocsToImport);
    public void cancel();
}

