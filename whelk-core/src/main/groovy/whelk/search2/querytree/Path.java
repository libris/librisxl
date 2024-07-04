package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.Disambiguate.RDF_TYPE;

public record Path(List<String> path, Optional<String> nestedStem) {
    // TODO: Get substitutions from context instead?
    private static final Map<String, String> substitutions = Map.of(
            RDF_TYPE, JsonLd.TYPE_KEY,
            "hasItem", String.format("%s.itemOf", JsonLd.REVERSE_KEY),
            "hasInstance", String.format("%s.instanceOf", JsonLd.REVERSE_KEY)
    );

    public Path(List<String> path) {
        this(path, Optional.empty());
    }

    public String asString() {
        return path.stream().map(this::substitute).collect(Collectors.joining("."));
    }

    public Path prepend(List<String> keys) {
        return new Path(Stream.concat(keys.stream(), path.stream()).toList());
    }

    public Path append(String key) {
        return new Path(Stream.concat(path.stream(), Stream.of(key)).toList());
    }

    public Path insertNested(Function<String, Optional<String>> getNestedPath) {
        return new Path(path, getNestedPath.apply(asString()));
    }

    private String substitute(String property) {
        return Optional.ofNullable(substitutions.get(property)).orElse(property);
    }
}
