package whelk.plugin;

import whelk.plugin.WhelkAware;

import java.util.List;
import whelk.result.ImportResult;

public interface Importer extends WhelkAware {
    ImportResult doImport(String dataset, int maxNrOfDocsToImport);
    public int getRecordCount();
    public void cancel();
}

