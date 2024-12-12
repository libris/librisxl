package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.Disambiguate.Rdfs.RDF_TYPE;

public record Path(List<Object> path, Optional<String> nestedStem) {
    // TODO: Get substitutions from context instead?
    private static final Map<String, String> substitutions = Map.of(
            RDF_TYPE, JsonLd.TYPE_KEY,
            "hasItem", String.format("%s.itemOf", JsonLd.REVERSE_KEY),
            "hasInstance", String.format("%s.instanceOf", JsonLd.REVERSE_KEY)
    );

    public Path(List<Object> path) {
        this(path, Optional.empty());
    }

    @Override
    public String toString() {
        return path.stream()
                .map(x -> x instanceof Property ? ((Property) x).name() : (String) x)
                .map(this::substitute)
                .collect(Collectors.joining("."));
    }

    public Optional<Property> mainProperty() {
        List<?> properties = path.stream().filter(x -> x instanceof Property).toList();
        return properties.isEmpty() ? Optional.empty() : Optional.of((Property) properties.getLast());
    }

    public Path prepend(List<Object> subpath) {
        return new Path(Stream.concat(subpath.stream(), path.stream()).toList());
    }

    public Path append(Object o) {
        return new Path(Stream.concat(path.stream(), Stream.of(o)).toList());
    }

    public Path insertNested(Function<String, Optional<String>> getNestedPath) {
        return new Path(path, getNestedPath.apply(toString()));
    }

    public boolean hasIdOrSearchKey() {
        return path.getLast().equals(JsonLd.ID_KEY) || path.getLast().equals(JsonLd.SEARCH_KEY);
    }

    private String substitute(String property) {
        return Optional.ofNullable(substitutions.get(property)).orElse(property);
    }
}
