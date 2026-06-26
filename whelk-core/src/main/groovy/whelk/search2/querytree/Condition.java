package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Operator;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.hash;
import static whelk.search2.Operator.EQUALS;
import static whelk.search2.Operator.LIKE;
import static whelk.search2.QueryUtil.parenthesize;

public non-sealed class Condition implements Node {
    private final Selector selector;
    private final Operator operator;
    private final Value value;

    public Condition(Selector selector, Operator operator, Value value) {
        this.selector = selector;
        this.operator = operator;
        this.value = value;
    }

    public Condition(String key, Operator operator, Value value) {
        this(new Key.RecognizedKey(new Token.Raw(key)), operator, value);
    }

    public Selector selector() {
        return selector;
    }

    public Operator operator() {
        return operator;
    }

    public Value value() {
        return value;
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return selector.isValid()
                ? expandWithAltSelectors(jsonLd, rdfSubjectTypes)
                : ExpandedNode.identity(this);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink, BiFunction<Node, Node, Map<String, String>> makeReplaceLink) {
        return _toSearchMapping(makeUpLink, makeReplaceLink);
    }

    @Override
    public String toQueryString(boolean topLevel) {
        var k = selector.formattedQueryKey();
        var v = value.isMultiToken() ? parenthesize(value.queryForm()) : value.queryForm();
        return operator.format(k, v);
    }

    @Override
    public Node getInverse() {
        return operator.isRange() ? withOperator(operator.getInverse()) : new Not(this);
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
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Condition other && hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        return hash(selector, operator, value);
    }

    public Condition withSelector(Selector s) {
        return new Condition(s, operator, value);
    }

    public Condition withOperator(Operator op) {
        return new Condition(selector, op, value);
    }

    public Condition withValue(Value v) {
        return new Condition(selector, operator, v);
    }

    public boolean isTypeNode() {
        return selector instanceof Property.RdfType && operator.equals(EQUALS) && value instanceof VocabTerm;
    }

    public Type asTypeNode() {
        return new Type((Property.RdfType) selector, (VocabTerm) value);
    }

    private ExpandedNode expandWithAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        List<Node> withAltSelectors = selector.getAltSelectors(jsonLd, rdfSubjectTypes, true).stream()
                .map(this::withSelector)
                .map(s -> s._expand(jsonLd))
                .toList();
        Node expanded = withAltSelectors.size() > 1 ? new Or(withAltSelectors) : withAltSelectors.getFirst();
        return new ExpandedNode(expanded, Map.of(this, expanded));
    }

    private Node _expand(JsonLd jsonLd) {
        List<? extends PathElement> path = selector.path();

        List<Node> expanded = Stream.concat(Stream.of(withSelector(path.size() > 1 ? new Path(path) : path.getFirst())), getPrefilledFields(path).stream())
                .map(s -> s.expandType(jsonLd))
                .toList();

        return expanded.size() > 1 ? new And(expanded) : expanded.getFirst();
    }

    private List<Condition> getPrefilledFields(List<? extends PathElement> path) {
        List<Condition> prefilledFields = new ArrayList<>();
        List<PathElement> currentPath = new ArrayList<>();
        for (PathElement pe : path) {
            currentPath.add(pe);
            if (pe instanceof Property.RestrictedSubProperty p && !p.hasIndexKey()) {
                for (Restrictions.HasValue r : p.getObjectRestrictions()) {
                    var restrictedPath = new Path(Stream.concat(currentPath.stream(), r.onProperty().path().stream()).toList());
                    prefilledFields.add(new Condition(restrictedPath, EQUALS, r.value()));
                }
            }
        }
        return prefilledFields;
    }

    // When querying type, match any subclass by default (TODO: make this optional)
    private Node expandType(JsonLd jsonLd) {
        if (!(selector.isType() && value instanceof VocabTerm v)) {
            return this;
        }

        String baseType = v.key();

        Set<String> subtypes = jsonLd.getSubClasses(baseType);
        if (subtypes.isEmpty()) {
            return this;
        }

        List<Condition> altFields = Stream.concat(Stream.of(baseType), subtypes.stream())
                .filter(Predicate.not(jsonLd::isDeprecated))
                .sorted()
                .map(t -> withValue(new VocabTerm(t, jsonLd.vocabIndex.get(t))))
                .toList();

        return new Or(altFields);
    }

    private Map<String, Object> _toSearchMapping(Function<Node, Map<String, String>> makeUpLink, BiFunction<Node, Node, Map<String, String>> makeReplaceLink) {
        Map<String, Object> m = new LinkedHashMap<>();

        m.put("property", selector.definition());
        m.put(operator.termKey, value instanceof Resource r ? r.description() : value.queryForm());
        m.put("up", makeUpLink.apply(this));

        if (operator == LIKE) {
            m.put("toEquals", makeReplaceLink.apply(this, new Condition(selector, EQUALS, value)));
        }
        if (operator == EQUALS && selector instanceof Property p && !(value instanceof FreeText) && p.isPreferLike()) {
            m.put("toLike", makeReplaceLink.apply(this, new Condition(selector, LIKE, value)));
        }

        m.put("_key", selector.queryKey());
        m.put("_value", value.queryForm());

        return m;
    }
}
