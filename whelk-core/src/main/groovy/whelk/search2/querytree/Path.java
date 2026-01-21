package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.QueryUtil;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.Owl.INVERSE_OF;
import static whelk.JsonLd.Owl.PROPERTY_CHAIN_AXIOM;

public final class Path implements Selector {
    private final List<PathElement> path;
    private Token token;

    public Path(List<? extends PathElement> path, Token token) {
        this(path);
        this.token = token;
    }

    public Path(List<? extends PathElement> path) {
        this.path = path.stream().map(PathElement.class::cast).toList();
    }

    public Path(PathElement pe) {
        this.path = List.of(pe);
    }

    @Override
    public String queryKey() {
        if (token != null) {
            return token.formatted();
        }
        String s = path.stream().map(Selector::queryKey).collect(Collectors.joining("."));
        return s.contains(":") ? QueryUtil.quote(s) : s;
    }

    @Override
    public String esField() {
        return path.stream().map(Selector::esField).collect(Collectors.joining("."));
    }

    @Override
    public List<? extends PathElement> path() {
        return path.stream().flatMap(pe -> pe.path().stream()).toList();
    }

    @Override
    public List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return getAltPaths(path(), jsonLd, rdfSubjectTypes).stream()
                .map(l -> l.size() > 1 ? new Path(l) : l.getFirst())
                .toList();
    }

    private List<List<PathElement>> getAltPaths(List<? extends PathElement> tail, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        if (tail.isEmpty()) {
            return List.of(List.of());
        }
        var next = tail.getFirst();
        var newTail = tail.subList(1, tail.size());
        var nextAltSelectors = next.getAltSelectors(jsonLd, rdfSubjectTypes);
        if (nextAltSelectors.isEmpty()) {
            // Indicates that an integral relation has been canceled out by its reverse
            // For example, when instanceOf is prepended to hasInstance.x
            // the integral property is dropped and only the tail (x) is kept
            return getAltPaths(newTail, jsonLd, List.of());
        }
        return nextAltSelectors.stream()
                .flatMap(s -> getAltPaths(newTail, jsonLd, next.range()).stream()
                        .filter(altPath ->
                                // Avoid creating alternative paths caused by inverse integral round-trips.
                                // For example, if the original path is hasInstance.x, do not
                                // generate the alternative path x via instanceOf.hasInstance.x.
                                !(s instanceof Property p1
                                        && !altPath.isEmpty() && altPath.getFirst() instanceof Property p2
                                        && p1.isInverseOf(p2)))
                        .map(altPath -> Stream.concat(s.path().stream(), altPath.stream())))
                .map(Stream::toList)
                .toList();
    }

    @Override
    public boolean isValid() {
        return path.stream().allMatch(Selector::isValid);
    }

    @Override
    public boolean isType() {
        return last().isType();
    }

    @Override
    public boolean isObjectProperty() {
        return last().isObjectProperty();
    }

    @Override
    public boolean mayAppearOnType(String type, JsonLd jsonLd) {
        return first().mayAppearOnType(type, jsonLd);
    }

    @Override
    public boolean appearsOnType(String type, JsonLd jsonLd) {
        return first().appearsOnType(type, jsonLd);
    }

    @Override
    public boolean indirectlyAppearsOnType(String type, JsonLd jsonLd) {
        return first().indirectlyAppearsOnType(type, jsonLd);
    }

    @Override
    public boolean appearsOnlyOnRecord(JsonLd jsonLd) {
        return first().appearsOnlyOnRecord(jsonLd);
    }

    @Override
    public Map<String, Object> definition() {
        LinkedList<Map<String, Object>> propertyChainAxiom = new LinkedList<>();
        for (int i = path.size() - 1; i >= 0; i--) {
            var step = path.get(i);
            if (step instanceof Property p && i > 0 && path.get(i - 1).queryKey().equals(JsonLd.REVERSE_KEY)) {
                propertyChainAxiom.push(Map.of(INVERSE_OF, p.definition()));
                i--;
            } else {
                propertyChainAxiom.push(step.definition());
            }
        }
        return propertyChainAxiom.size() > 1
                ? Map.of(PROPERTY_CHAIN_AXIOM, List.of(Map.of(JsonLd.LIST_KEY, propertyChainAxiom)))
                : propertyChainAxiom.getFirst();
    }

    @Override
    public List<String> domain() {
        return first().domain();
    }

    @Override
    public List<String> range() {
        return last().range();
    }

    public Token token() {
        return token;
    }

    @Override
    public String toString() {
        return path.stream()
                .map(Selector::toString)
                .collect(Collectors.joining("."));
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Path p && p.path().equals(path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    private Selector first() {
        return path.getFirst();
    }

    public Selector last() {
        return path.getLast();
    }
}
