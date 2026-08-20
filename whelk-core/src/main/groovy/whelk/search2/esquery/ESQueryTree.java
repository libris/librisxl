package whelk.search2.esquery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record ESQueryTree(ESNode tree) {
    public Map<String, Object> dsl() {
        return new LinkedHashMap<>(Map.of("query", tree.dsl()));
    }

    public Map<String, Object> dslWithPostFilter() {
        ESNode factored = factorOutPostFilter(tree);

        if (factored instanceof ESNode.PostFilter postFilter) {
            Map<String, Object> dsl = new LinkedHashMap<>();
            dsl.put("query", new ESNode.MatchAll().dsl());
            dsl.put("post_filter", postFilter.dsl());
            return dsl;
        }

        if (factored instanceof ESNode.Group group) {
            Optional<ESNode> postFilter = group.subQueries().stream()
                    .filter(ESNode.PostFilter.class::isInstance)
                    .findFirst();
            if (postFilter.isPresent()) {
                List<ESNode> subQueries = new ArrayList<>(group.subQueries());
                subQueries.remove(postFilter.get());
                ESNode mainQuery = subQueries.size() == 1 ? subQueries.getFirst() : group.withSubQueries(subQueries);
                Map<String, Object> dsl = new LinkedHashMap<>();
                dsl.put("query", mainQuery.dsl());
                dsl.put("post_filter", postFilter.get().dsl());
                return dsl;
            }
        }

        return new LinkedHashMap<>(Map.of("query", tree.dsl()));
    }

    public ESQueryTree add(List<ESNode> queries) {
        Stream<ESNode> subQueriesStream = tree instanceof ESNode.Must(List<ESNode> subQueries)
                ? subQueries.stream()
                : Stream.of(tree);
        List<ESNode> newSubQueries = Stream.concat(subQueriesStream, queries.stream()).toList();
        return new ESQueryTree(new ESNode.Must(newSubQueries));
    }

    private static ESNode factorOutPostFilter(ESNode query) {
        return switch (query) {
            case ESNode.Nested nested -> factorOutPostFilter(nested);
            case ESNode.Group group -> factorOutPostFilter(group);
            default -> query;
        };
    }

    private static ESNode factorOutPostFilter(ESNode.Nested nested) {
        return switch (nested.query()) {
            case ESNode.PostFilter postFilter -> liftPostFilter(nested, postFilter);
            case ESNode.Group group -> {
                ESNode factored = factorOutPostFilter(group);
                yield switch (factored) {
                    case ESNode.PostFilter postFilter ->
                            liftPostFilter(nested, postFilter);
                    case ESNode.Group g when g.subQueries().stream().anyMatch(ESNode.PostFilter.class::isInstance) -> {
                        ESNode innerQuery = group.withSubQueries(unwrapPostFilters(g.subQueries()));
                        yield new ESNode.PostFilter(nested.withInnerQuery(innerQuery));
                    }
                    default -> nested;
                };
            }

            default -> nested;
        };
    }

    private static ESNode factorOutPostFilter(ESNode.Group group) {
        List<ESNode> subQueries = group.subQueries().stream()
                .map(ESQueryTree::factorOutPostFilter)
                .collect(Collectors.toList());
        List<ESNode> postFilterNodes = subQueries.stream()
                .filter(ESNode.PostFilter.class::isInstance)
                .toList();

        if (!postFilterNodes.isEmpty()) {
            subQueries.removeAll(postFilterNodes);
            postFilterNodes = unwrapPostFilters(postFilterNodes);
            ESNode postFilterQuery = postFilterNodes.size() == 1
                    ? postFilterNodes.getFirst()
                    : group.withSubQueries(postFilterNodes);
            subQueries.add(new ESNode.PostFilter(postFilterQuery));
        }

        return subQueries.size() == 1
                ? subQueries.getFirst()
                : group.withSubQueries(subQueries);
    }

    private static List<ESNode> unwrapPostFilters(List<ESNode> queries) {
        return queries.stream()
                .map(query -> query instanceof ESNode.PostFilter(ESNode inner)
                        ? inner
                        : query)
                .toList();
    }

    private static ESNode.PostFilter liftPostFilter(ESNode.Nested nested, ESNode.PostFilter postFilter) {
        return new ESNode.PostFilter(nested.withInnerQuery(postFilter.query()));
    }
}
