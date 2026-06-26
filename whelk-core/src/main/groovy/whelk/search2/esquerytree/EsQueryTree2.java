package whelk.search2.esquerytree;

import whelk.search2.ESSettings;
import whelk.search2.SelectedFacets;
import whelk.search2.querytree.ExpandedQueryTree;

import java.util.Map;

public class EsQueryTree2 {
    private final EsQuery mainQuery;
    private final EsQuery postFilter;

    public EsQueryTree2(ExpandedQueryTree queryTree, ESSettings esSettings) {
        this(queryTree, esSettings, null);
    }

    public EsQueryTree2(ExpandedQueryTree queryTree, ESSettings esSettings, SelectedFacets selectedFacets) {
        this.mainQuery = EsQueryTreeBuilder.buildFrom(queryTree, esSettings);
        this.postFilter = new EsQuery.MatchAll();
    }

    public Map<String, Object> getMainQuery() {
        return mainQuery.dsl();
    }

    public Map<String, Object> getPostFilter() {
        return postFilter.dsl();
    }
}
