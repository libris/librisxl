package whelk.search2.querytree;

import java.util.List;

public record EsQuery(Object dsl, List<String> indexNames) {
}
