package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.EsBoost;
import whelk.search2.EsMappings;
import whelk.search2.QueryParams;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public sealed interface Node permits ActiveFilter, FreeText, Group, InactiveFilter, PathValue {
    Map<String, Object> toEs(EsMappings esMappings, EsBoost.Config boostConfig);

    Node expand(JsonLd jsonLd, Collection<String> rulingTypes);

    Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams);

    String toQueryString(boolean topLevel);

    Node getInverse();

    boolean shouldContributeToEsScore();

    default List<Node> children() {
        return Collections.emptyList();
    }

    default Node reduceTypes(JsonLd jsonLd) {
        return this;
    }

    default boolean isTypeNode() {
        return false;
    }

    default boolean isFreeTextNode() {
        return false;
    }
}