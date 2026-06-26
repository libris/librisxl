package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Query;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.search2.Query.Connective.AND;
import static whelk.search2.Query.Connective.OR;
import static whelk.search2.QueryUtil.parenthesize;
import static whelk.search2.Operator.EQUALS;

public record FreeText(Property.TextQuery textQuery, List<Token> tokens, Query.Connective connective) implements Node, Value {
    public FreeText(Property.TextQuery textQuery, Token token) {
        this(textQuery, List.of(token), AND);
    }

    public FreeText(Token token) {
        this(null, token);
    }

    public FreeText(String s) {
        this(new Token.Raw(s));
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return ExpandedNode.identity(this);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink, BiFunction<Node, Node, Map<String, String>> makeReplaceLink) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("property", textQuery != null ? textQuery.definition() : Map.of());
        m.put(EQUALS.termKey, queryForm());
        m.put("up", makeUpLink.apply(this));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        String s = joinTokens();
        return isMultiToken() && !topLevel && connective == OR ? parenthesize(s) : s;
    }

    @Override
    public String queryForm() {
        return joinTokens();
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public Node getInverse() {
        return new Not(this);
    }

    @Override
    public Node reduce(JsonLd jsonLd) {
        return this;
    }

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return implies(node, this::equals);
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
    }

    @Override
    public boolean isMultiToken() {
        return tokens.size() > 1;
    }

    @Override
    public boolean isRangeOpCompatible() {
        return tokens.size() == 1 && tokens.getFirst().isDigits();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FreeText ft && ft.toString().equals(toString());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(toString());
    }

    public String toEsString() {
        return joinTokens(tokens, " ");
    }

    public FreeText withTokens(List<Token> tokens) {
        return new FreeText(textQuery, tokens, connective);
    }

    public boolean isDigits() {
        return tokens.stream().allMatch(Token::isDigits);
    }

    private String joinTokens() {
        return switch (connective) {
            case AND -> joinTokens(tokens, " ");
            case OR -> joinTokens(tokens, " OR ");
        };
    }

    private static String joinTokens(List<Token> tokens, String delimiter) {
        return tokens.stream().map(Token::formatted).collect(Collectors.joining(delimiter));
    }
}
