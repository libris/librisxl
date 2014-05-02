package se.kb.libris.whelks.component;

public interface HybridStorage extends Storage {
    public Index getIndex();
    public void setIndex(Index index);
    public void rebuildIndex();
}
