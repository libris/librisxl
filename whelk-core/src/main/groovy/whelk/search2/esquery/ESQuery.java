package whelk.search2.esquery;

import whelk.component.ElasticSearch;
import whelk.search2.QueryUtil;

import java.util.List;
import java.util.Map;

public record ESQuery(Map<String, Object> dsl, List<String> indexNames, ElasticSearch elasticSearch) {
    public Map<String, Object> run() {
        var response = elasticSearch.query(dsl, indexNames);
        return QueryUtil.castToStringObjectMap(response);
    }
}
