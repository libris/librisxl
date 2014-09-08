package se.kb.libris.whelks.plugin;

import java.util.List;

public interface Importer extends WhelkAware {
    int doImport(String dataset, String token, int maxNrOfDocsToImport, boolean silent, boolean picky);
    public void cancel();
}

