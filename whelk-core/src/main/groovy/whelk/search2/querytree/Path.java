package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.QueryUtil;

import java.util.ArrayList;
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
    public Path expand(JsonLd jsonLd) {
        List<Selector> expandedPath = new ArrayList<>();

        for (Selector step : path) {
            List<Selector> expandedStep = new ArrayList<>(step.expand(jsonLd).path());
            if (!expandedPath.isEmpty() && expandedPath.getLast() instanceof Property p1 && expandedStep.getFirst() instanceof Property p2) {
                if (p1.isInverseOf(p2)) {
                    // e.g. when the original path is instanceOf.x and x expands to hasInstance.y
                    // then we need to adjust the expanded path instanceOf.hasInstance.y -> y
                    expandedPath.removeLast();
                    expandedStep.removeFirst();
                } else if (JsonLd.RECORD_KEY.equals(p1.name()) && JsonLd.RECORD_KEY.equals(p2.name())) {
                    // when the original path is meta.x and x expands to meta.x
                    // then we need to adjust the expanded path meta.meta.x -> meta.x
                    expandedStep.removeFirst();
                }
            }
            expandedPath.addAll(expandedStep);
        }
        return new Path(expandedPath);
    }

    @Override
    public List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return getAltPaths(path, jsonLd, rdfSubjectTypes).stream()
                .map(Path::new)
                .map(Selector.class::cast)
                .toList();
    }

    private List<List<Selector>> getAltPaths(List<Selector> tail, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        if (tail.isEmpty()) {
            return List.of(List.of());
        }
        Selector next = tail.getFirst();
        List<Selector> newTail = tail.subList(1, tail.size());
        return next.getAltSelectors(jsonLd, rdfSubjectTypes).stream()
                .flatMap(s -> getAltPaths(newTail, jsonLd, next.range()).stream()
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
                ? Map.of(PROPERTY_CHAIN_AXIOM, Map.of(JsonLd.LIST_KEY, propertyChainAxiom))
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
