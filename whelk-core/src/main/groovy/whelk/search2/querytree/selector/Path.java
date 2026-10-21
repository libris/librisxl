package whelk.search2.querytree.selector;

import whelk.JsonLd;
import whelk.search2.querytree.value.Token;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
    public Token token() {
        if (token != null) {
            return token;
        }
        List<Token> tokens = path.stream().map(PathElement::token).toList();
        String s = tokens.stream().map(Token::value).collect(Collectors.joining("."));
        return tokens.stream().anyMatch(Token::isQuoted) ? new Token.Quoted(s) : new Token.Raw(s);
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
    public boolean isValid() {
        return path.stream().allMatch(Selector::isValid);
    }

    @Override
    public boolean isType() {
        return last().isType();
    }

    @Override
    public boolean isComposite() {
        return last().isComposite();
    }

    @Override
    public boolean isObjectProperty() {
        return last().isObjectProperty();
    }

    @Override
    public boolean isLdSetContainer() {
        return last().isLdSetContainer();
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

    public Token originalToken() {
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
