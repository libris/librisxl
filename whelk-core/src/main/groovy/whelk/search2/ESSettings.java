package whelk.search2;

import whelk.Whelk;

public class ESSettings {
    EsMappings mappings;

    private int maxItems;

    public ESSettings(Whelk whelk) {
        if (whelk.elastic != null) {
            this.mappings = new EsMappings(whelk.elastic.getMappings());
            this.maxItems = whelk.elastic.maxResultWindow;;
        }
    }

    public boolean isConfigured() {
        return mappings != null;
    }

    public int maxItems() {
        return maxItems;
    }
}
