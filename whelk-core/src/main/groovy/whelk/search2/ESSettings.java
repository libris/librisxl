package whelk.search2;

import whelk.Whelk;

public class ESSettings {
    EsMappings mappings;
    EsBoost boost;

    private int maxItems;

    public ESSettings(Whelk whelk) {
        if (whelk.elastic != null) {
            this.mappings = new EsMappings(whelk.elastic.getMappings());
            this.maxItems = whelk.elastic.maxResultWindow;;
        }
        this.boost = new EsBoost(whelk.getJsonld());
    }

    public boolean isConfigured() {
        return mappings != null;
    }

    public int maxItems() {
        return maxItems;
    }
}
