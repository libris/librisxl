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
    private final List<Selector> path;
    private Token token;

    public Path(List<Selector> path, Token token) {
        this.path = path;
        this.token = token;
    }

    public Path(List<Selector> path) {
        this.path = path;
    }

    public Path(Selector s) {
        this.path = List.of(s);
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
    public List<Selector> path() {
        return path;
    }

    @Override
    public Selector expand(JsonLd jsonLd) {
        List<Selector> expandedPath = path.stream()
                .flatMap(s -> s.expand(jsonLd).path().stream())
                .toList();
        // TODO: Explanation + example
        if (expandedPath.size() > 2
                && expandedPath.get(0) instanceof Property p1
                && expandedPath.get(1) instanceof Property p2
                && p1.isInverseOf(p2)) {
            expandedPath = expandedPath.subList(2, expandedPath.size());
        }
        return new Path(expandedPath);
    }

    @Override
    public List<Selector> getAltPaths(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return first().getAltPaths(jsonLd, rdfSubjectTypes).stream()
                .map(s -> Stream.concat(s.path().stream(), tail().stream()).toList())
                .map(Path::new)
                .map(Selector.class::cast)
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
    public boolean valueIsObject() {
        return last().valueIsObject();
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
    public int offset() {
        return token != null ? token.offset() : -1;
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
                ? Map.of(PROPERTY_CHAIN_AXIOM, propertyChainAxiom)
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

    private List<Selector> tail() {
        return path.subList(1, path.size());
    }
}
