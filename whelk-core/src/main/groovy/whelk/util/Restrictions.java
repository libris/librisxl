package whelk.util;

import whelk.Document;
import whelk.JsonLd;
import whelk.Link;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;


public class Restrictions {
    public record Restriction(String parentProperty, String subProperty, String indexTerm) { }

    public record Narrowed(Restriction restriction, String name, String indexKey) {
        public String propertyKey() {
            return restriction.indexTerm + "." + indexKey;
        }
    }
    
    // TODO get from vocab
    private static final Restriction CATEGORY =
            new Restriction("category", "inCollection", "_categoryByCollection");

    private static final List<Restriction> RESTRICTED = List.of(CATEGORY);

    private static final List<Narrowed> NARROWED_LIST = List.of(
            new Narrowed(CATEGORY, "findCategory", "find"),
            new Narrowed(CATEGORY, "identifyCategory", "identify"),
            new Narrowed(CATEGORY, "noneCategory", JsonLd.NONE_KEY)
    );

    /**
     * for example: [ category : [ saogf:Poesi : [identifyCategory] ] ]
     */
    private final Map<String, Map<String, List<Narrowed>>> subPropertyByValueByProperty = new HashMap<>();

    public Restriction tryFindRestriction(String propertyName) {
        return RESTRICTED.stream()
                .filter(p -> Objects.equals(p.parentProperty(), propertyName))
                .findFirst()
                .orElse(null);
    }

    /**
     *
     * @param name e.g. findCategory
     */
    public Narrowed tryFindNarrowByNarrowName(String name) {
        return NARROWED_LIST.stream()
                .filter(p -> Objects.equals(p.name(), name))
                .findFirst()
                .orElse(null);
    }

    public Narrowed tryNarrow(Link link) {
        var property = Link.relation(link);
        var iri = Link.iri(link);

        var narrowedByValue = subPropertyByValueByProperty.get(property);
        if (narrowedByValue == null) {
            throw new IllegalArgumentException("no restriction for: " + property);
        }

        return narrowedByValue.getOrDefault(iri, List.of())
                .stream()
                .findFirst()
                .orElseGet(() ->
                        NARROWED_LIST.stream()
                                .filter(narrowed ->
                                        Objects.equals(property, narrowed.restriction().parentProperty())
                                        && Objects.equals(JsonLd.NONE_KEY, narrowed.indexKey()))
                                .findFirst()
                                .orElse(null)
                );
    }

    public void init(Function<String, Iterable<Document>> loadDocsByType, JsonLd jsonLd) {
        for (var entry : NARROWED_LIST.stream().collect(Collectors.groupingBy(Narrowed::restriction)).entrySet()) {
            Restriction restriction = entry.getKey();
            List<Narrowed> narrowedList = entry.getValue();

            var path = new ArrayList<>(List.of(JsonLd.GRAPH_KEY, 1));
            var key = restriction.subProperty();
            path.add(key);

            var allIndexKeys = narrowedList.stream().map(Narrowed::indexKey).toList();

            var types = new HashSet<String>();
            jsonLd.getRange(restriction.parentProperty()).forEach(type -> {
                types.add(type);
                types.addAll(jsonLd.getSubClasses(type));
            });

            for (String type : types) {
                for (var doc : loadDocsByType.apply(type)) {
                    var iri = doc.getThingIdentifiers().stream().findFirst().orElseThrow();

                    @SuppressWarnings("unchecked")
                    var things = ((List<Map<?, ?>>) JsonLd.asList(DocumentUtil.getAtPath(doc.data, path, List.of())));
                    List<String> values = things.stream()
                            .map(m -> (String) m.get(JsonLd.ID_KEY))
                            .filter(Objects::nonNull)
                            .map(Restrictions::toIndexKey)
                            .filter(allIndexKeys::contains)
                            .toList();

                    for (var value : values) {
                        var narrowedByValue = subPropertyByValueByProperty
                                .computeIfAbsent(restriction.parentProperty(), k -> new HashMap<>());

                        var list = narrowedByValue
                                .computeIfAbsent(iri, k -> new ArrayList<>());

                        var matching = narrowedList.stream()
                                .filter(n -> Objects.equals(n.indexKey(), value))
                                .toList();

                        list.addAll(matching);
                    }
                }
            }
        }
    }

    private static String toIndexKey(String iri) {
        int lastSlash = iri.lastIndexOf('/');
        return lastSlash >= 0 ? iri.substring(lastSlash + 1) : iri;
    }
}
