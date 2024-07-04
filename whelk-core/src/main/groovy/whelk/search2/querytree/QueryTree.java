package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.OutsetType;
import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;
import whelk.search2.parse.Ast;
import whelk.search2.parse.FlattenedAst;
import whelk.search2.parse.Lex;
import whelk.search2.parse.Parse;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static whelk.search2.querytree.PropertyValue.equalsLink;
import static whelk.search2.QueryUtil.encodeUri;
import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;

public class QueryTree {
    public Node tree;
    private QueryTree filtered;
    private String freeTextPart;
    private OutsetType outsetType;

    public QueryTree(String queryString, Disambiguate disambiguate,
                     Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        if (!queryString.isEmpty()) {
            LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
            Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
            Ast ast = new Ast(parseTree);
            FlattenedAst flattened = new FlattenedAst(ast);
            this.tree = buildTree(flattened.tree, disambiguate, aliasToFilter);
            normalizeFreeText();
            removeNeedlessWildcard();
        }
    }

    public QueryTree(Node tree) {
        this.tree = tree;
        removeNeedlessWildcard();
    }

    public Map<String, Object> toEs(QueryUtil queryUtil, Disambiguate disambiguate) {
        return (isFiltered() ? filtered.tree : tree)
                .expand(disambiguate, getOutsetType(disambiguate))
                .insertNested(queryUtil::getNestedPath)
                .toEs(queryUtil.lensBoost.computeBoostFieldsFromLenses(new String[0])); // TODO: Implement boosting
    }

    public Map<String, Object> toSearchMapping(Function<Value, Object> lookUp, Map<String, String> nonQueryParams) {
        return isEmpty()
                ? Collections.emptyMap()
                : tree.toSearchMapping(this, lookUp, nonQueryParams);
    }

    public Map<String, String> makeUpLink(Node n, Map<String, String> nonQueryParams) {
        QueryTree reducedTree = excludeFromTree(n);
        String upUrl = QueryUtil.makeFindUrl(reducedTree, nonQueryParams);
        return Map.of(JsonLd.ID_KEY, upUrl);
    }

    public OutsetType getOutsetType() {
        if (outsetType == null) {
            throw new RuntimeException("outsetType is null");
        }
        return outsetType;
    }

    private OutsetType getOutsetType(Disambiguate disambiguate) {
        this.outsetType = disambiguate.decideOutset(isFiltered() ? filtered : this);
        return outsetType;
    }

    /**
     * There is no freetext or all freetext nodes are "*"
     */
    public boolean isWild() {
        return StreamSupport.stream(allDescendants(tree).spliterator(), false)
                .noneMatch(n -> n instanceof FreeText && !((FreeText) n).isWild());
    }

    private static Iterable<Node> allDescendants(Node node) {
        Iterator<Node> i = new Iterator<>() {
            List<Node> nodes;

            @Override
            public boolean hasNext() {
                if (nodes == null) {
                    nodes = new LinkedList<>();
                    nodes.add(node);
                }
                return !nodes.isEmpty();
            }

            @Override
            public Node next() {
                Node next = nodes.removeFirst();
                nodes.addAll(next.children());
                return next;
            }
        };

        return () -> i;
    }

    private static Node buildTree(FlattenedAst.Node ast, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        return switch (ast) {
            case FlattenedAst.And and -> buildAnd(and, disambiguate, aliasToFilter);
            case FlattenedAst.Or or -> buildOr(or, disambiguate, aliasToFilter);
            case FlattenedAst.Not not -> buildFromNot(not, aliasToFilter);
            case FlattenedAst.Leaf l -> buildFromLeaf(l, aliasToFilter);
            case FlattenedAst.Code c -> buildFromCode(c, disambiguate);
        };
    }

    private static Node buildAnd(FlattenedAst.And and, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        List<Node> conjuncts = new ArrayList<>();
        for (FlattenedAst.Node o : and.operands()) {
            conjuncts.add(buildTree(o, disambiguate, aliasToFilter));
        }
        return new And(conjuncts);
    }

    private static Node buildOr(FlattenedAst.Or or, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        List<Node> disjuncts = new ArrayList<>();
        for (FlattenedAst.Node o : or.operands()) {
            disjuncts.add(buildTree(o, disambiguate, aliasToFilter));
        }
        return new Or(disjuncts);
    }

    private static Node buildFromNot(FlattenedAst.Not not, Map<String, AppParams.Filter> aliasToFilter) {
        return aliasToFilter.containsKey(not.value())
                ? new InactiveBoolFilter(not.value())
                : new FreeText(Operator.NOT_EQUALS, not.value());
    }

    private static Node buildFromLeaf(FlattenedAst.Leaf leaf, Map<String, AppParams.Filter> aliasToFilter) {
        var filter = aliasToFilter.get(leaf.value());
        if (filter == null) {
            return new FreeText(Operator.EQUALS, leaf.value());
        }
        return new ActiveBoolFilter(leaf.value(), filter.getExplicit(), filter.getPrefLabelByLang());
    }

    private static Node buildFromCode(FlattenedAst.Code c, Disambiguate disambiguate) throws InvalidQueryException {
        Optional<String> property = disambiguate.mapToProperty(c.code());
        if (property.isPresent()) {
            Property p = new Property(property.get(), disambiguate.getQueryCode(property.get()));
            Value v = buildValue(property.get(), c.value(), disambiguate);
            return new PropertyValue(p, c.operator(), v);
        }
        return buildPathValue(c, disambiguate);
    }

    private static PathValue buildPathValue(FlattenedAst.Code c, Disambiguate disambiguate) throws InvalidQueryException {
        String property = null;
        String value = c.value();
        List<String> path = new ArrayList<>();

        for (String part : c.code().split("\\.")) {
            Optional<String> mappedProperty = disambiguate.mapToProperty(part);
            if (mappedProperty.isPresent()) {
                property = mappedProperty.get();
                path.add(property);
            } else if (Disambiguate.isLdKey(part) || JsonLd.SEARCH_KEY.equals(part)) {
                path.add(part);
            } else {
                var ambiguous = disambiguate.getAmbiguousPropertyMapping(part);
                if (ambiguous.isEmpty()) {
                    throw new InvalidQueryException("Unrecognized property alias: " + part);
                } else {
                    throw new InvalidQueryException("\"" + part + "\" maps to multiple properties: " + ambiguous + "," +
                            " please specify which one is meant.");
                }
            }
        }

        Value v = property == null ? new Literal(value) : buildValue(property, value, disambiguate);

        return new PathValue(path, c.operator(), v);
    }

    private static Value buildValue(String property, String value, Disambiguate disambiguate) throws InvalidQueryException {
        if (disambiguate.isType(property)) {
            Optional<String> mappedType = disambiguate.mapToKbvClass(value);
            if (mappedType.isPresent()) {
                return new VocabTerm(mappedType.get());
            } else {
                var ambiguous = disambiguate.getAmbiguousClassMapping(value);
                if (ambiguous.isEmpty()) {
                    throw new InvalidQueryException("Unrecognized type: " + value);
                } else {
                    throw new InvalidQueryException("\"" + value + "\" maps to multiple types: " + ambiguous + "," +
                            " please specify which one is meant.");
                }
            }
        } else if (disambiguate.isVocabTerm(property)) {
            Optional<String> mappedEnum = disambiguate.mapToEnum(value);
            if (mappedEnum.isPresent()) {
                return new VocabTerm(mappedEnum.get());
            } else {
                var ambiguous = disambiguate.getAmbiguousEnumMapping(value);
                if (ambiguous.isEmpty()) {
                    throw new InvalidQueryException("Invalid value " + value + " for property " + property);
                } else {
                    throw new InvalidQueryException("\"" + value + "\" maps to multiple types: " + ambiguous + "," +
                            " please specify which one is meant.");
                }
            }
        }
        // Expand and encode URIs, e.g. sao:Hästar -> https://id.kb.se/term/sao/H%C3%A4star
        else if (disambiguate.isObjectProperty(property)) {
            String expanded = Disambiguate.expandPrefixed(value);
            return JsonLd.looksLikeIri(expanded) ? new Link(encodeUri(expanded)) : new Literal(value);
        } else {
            return new Literal(value);
        }
    }

    private void normalizeFreeText() {
        this.tree = normalizeFreeText(tree);
    }

    private static Node normalizeFreeText(Node node) {
        return switch (node) {
            case And and -> {
                List<Node> conjuncts = new ArrayList<>();
                List<String> ftStrings = new ArrayList<>();
                for (Node n : and.children()) {
                    if (isFreeText(n)) {
                        ftStrings.add(quoteIfPhraseOrContainsSpecialSymbol(((FreeText) n).value()));
                    } else {
                        conjuncts.add(normalizeFreeText(n));
                    }
                }
                if (!ftStrings.isEmpty()) {
                    conjuncts.addFirst(new FreeText(Operator.EQUALS, String.join(" ", ftStrings)));
                }
                yield conjuncts.size() > 1 ? new And(conjuncts) : conjuncts.getFirst();
            }
            case Or or -> or.mapAndRebuild(QueryTree::normalizeFreeText);
            case FreeText ft -> new FreeText(ft.operator(), quoteIfPhraseOrContainsSpecialSymbol(ft.value()));
            case null, default -> node;
        };
    }

    public QueryTree andExtend(Node node) {
        return new QueryTree(addConjunct(tree, node));
    }

    static Node addConjunct(Node tree, Node node) {
        return switch (tree) {
            case null -> node;
            case And and -> and.add(node);
            default -> tree.equals(node) ? tree : new And(List.of(tree, node));
        };
    }

    public QueryTree excludeFromTree(Node node) {
        return new QueryTree(excludeFromTree(node, tree));
    }

    private static Node excludeFromTree(Node nodeToExclude, Node tree) {
        if (nodeToExclude == tree) {
            return null;
        }
        return switch (tree) {
            case Group group -> group.mapAndRebuild(c -> excludeFromTree(nodeToExclude, c), Objects::nonNull);
            default -> tree;
        };
    }

    public QueryTree removeTopLevelNode(Node node) {
        return new QueryTree(removeTopLevelNode(tree, node));
    }

    private static Node removeTopLevelNode(Node tree, Node node) {
        return switch (tree) {
            case And and -> and.remove(node);
            default -> node.equals(tree) ? null : tree;
        };
    }

    public QueryTree removeTopLevelPvRangeNodes(String property) {
        var rangeOps = Set.of(Operator.GREATER_THAN_OR_EQUALS, Operator.GREATER_THAN, Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUALS);
        return new QueryTree(removeTopLevelPvNodesByOperator(property, tree, rangeOps));
    }

    public QueryTree removeTopLevelPvNodesByOperator(String property) {
        return new QueryTree(removeTopLevelPvNodesByOperator(property, tree, Collections.emptySet()));
    }

    private static Node removeTopLevelPvNodesByOperator(String property, Node tree, Set<Operator> operators) {
        Predicate<Node> p = (node -> node instanceof PropertyValue
                && ((PropertyValue) node).property().equals(property)
                && (operators.isEmpty() || operators.contains(((PropertyValue) node).operator())));

        return switch (tree) {
            case null -> null;
            case And and -> and.mapAndRebuild(Function.identity(), Predicate.not(p));
            default -> p.test(tree) ? null : tree;
        };
    }

    public Set<String> collectGivenTypes() {
        return collectGivenTypes(tree, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(Node sqtNode, Set<String> types) {
        switch (sqtNode) {
            case And and -> and.children().forEach(c -> collectGivenTypes(c, types));
            case Or or -> or.children().forEach(d -> collectGivenTypes(d, types));
            case PropertyValue pv -> {
                if (pv.property().isRdfType()) {
                    types.add(pv.value().string());
                }
            }
            default -> {
                // Nothing to do here
            }
        }

        return types;
    }

    public boolean isEmpty() {
        return tree == null;
    }

    public boolean isFreeText() {
        return isFreeText(tree);
    }

    private static boolean isFreeText(Node node) {
        return node instanceof FreeText && ((FreeText) node).operator().equals(Operator.EQUALS);
    }

    public Set<InactiveBoolFilter> getInactiveBfNodes() {
        return getTopLevelNodes().stream()
                .filter(n -> n instanceof InactiveBoolFilter)
                .map(InactiveBoolFilter.class::cast)
                .collect(Collectors.toSet());
    }

    public Set<ActiveBoolFilter> getActiveBfNodes() {
        return getTopLevelNodes().stream()
                .filter(n -> n instanceof ActiveBoolFilter)
                .map(ActiveBoolFilter.class::cast)
                .collect(Collectors.toSet());
    }

    public List<PropertyValue> getTopLevelPvNodes() {
        return getTopLevelNodes().stream()
                .filter(n -> n instanceof PropertyValue)
                .map(PropertyValue.class::cast)
                .toList();
    }

    public Set<Node> getTopLevelNodes() {
        return switch (tree) {
            case And and -> new HashSet<>(and.children());
            case null -> Collections.emptySet();
            default -> Set.of(tree);
        };
    }

    public String getFreeTextPart() {
        if (freeTextPart == null) {
            if (tree instanceof And) {
                freeTextPart = tree.children()
                        .stream()
                        .filter(c -> c instanceof FreeText)
                        .map(FreeText.class::cast)
                        .filter(ft -> ft.operator() == Operator.EQUALS)
                        .map(FreeText::value)
                        .findFirst()
                        .orElse("");
            } else if (isFreeText()) {
                freeTextPart = ((FreeText) tree).value();
            } else {
                freeTextPart = "";
            }
        }

        return freeTextPart;
    }

    private void resetFreeTextPart() {
        this.freeTextPart = null;
    }

    public String toQueryString() {
        return isEmpty() ? "*" : tree.toString(true);
    }

    private void removeNeedlessWildcard() {
        if (!isFreeText() && Operator.WILDCARD.equals(getFreeTextPart())) {
            this.tree = removeTopLevelNode(tree, new FreeText(Operator.EQUALS, Operator.WILDCARD));
            resetFreeTextPart();
        }
    }

    public QueryTree normalizeFilters(AppParams.SiteFilters siteFilters) {
        return mapToAliases(siteFilters.aliasToFilter().values())
                .removeDefaultFilters(siteFilters.getAllDefaultFilterNodes())
                .removeNegatedSelectableFilters(siteFilters.getSelectableFilterAliases());
    }

    private QueryTree mapToAliases(Collection<AppParams.Filter> aliasedFilters) {
        if (isEmpty()) {
            return this;
        }

        var newTree = tree;

        for (var filter : aliasedFilters) {
            switch (newTree) {
                case And and -> {
                    if (and.contains(filter.getExplicit())) {
                        newTree = and.replace(filter.getExplicit(), filter.getAlias().get());
                    }
                }
                default -> {
                    if (filter.getExplicit().equals(newTree)) {
                        newTree = filter.getAlias().get();
                    }
                }
            }
        }

        return new QueryTree(newTree);
    }

    private QueryTree removeDefaultFilters(Collection<Node> defaultFilters) {
        var newTree = tree;

        for (var filter : defaultFilters) {
            switch (newTree) {
                case And and -> {
                    if (and.contains(filter)) {
                        newTree = and.remove(filter);
                    }
                }
                case null -> {}
                default -> {
                    if (filter.equals(newTree)) {
                        newTree = null;
                    }
                }
            }
        }

        return new QueryTree(newTree);
    }

    private QueryTree removeNegatedSelectableFilters(Collection<String> selectableFilterAliases) {
        var newTree = tree;

        for (var ibf : getInactiveBfNodes()) {
            if (selectableFilterAliases.contains(ibf.alias())) {
                switch (newTree) {
                    case And and -> newTree = and.remove(ibf);
                    case null -> {}
                    default -> newTree = null;
                }
            }
        }

        return new QueryTree(newTree);
    }

    public void addFilters(QueryParams queryParams, AppParams appParams) {
        var currentPvNodes = getTopLevelPvNodes();
        var currentActiveBfNodes = getActiveBfNodes();

        Function<PropertyValue, Boolean> isTypeEquals = pv ->
                pv.property().isRdfType() && pv.operator().equals(Operator.EQUALS);

        var newTree = tree;
        for (var node : getFilters(queryParams, appParams)) {
            switch (node) {
                case PropertyValue pv -> {
                    if (isTypeEquals.apply(pv)) {
                        if (currentPvNodes.stream().noneMatch(isTypeEquals::apply)) {
                            newTree = addConjunct(newTree, pv);
                        }
                    } else {
                        newTree = addConjunct(newTree, pv);
                    }
                }
                case ActiveBoolFilter abf -> {
                    if (currentActiveBfNodes.stream().noneMatch(bf -> bf.nullifies(abf))) {
                        newTree = addConjunct(newTree, abf);
                    }
                }
                default -> addConjunct(newTree, node);
            }
        }

        this.filtered = new QueryTree(newTree);
    }

    private boolean isFiltered() {
        return filtered != null;
    }

    private List<Node> getFilters(QueryParams queryParams, AppParams appParams) {
        var siteFilters = appParams.siteFilters;
        var object = queryParams.object;
        var predicates = queryParams.predicates;

        var filters = new ArrayList<>(siteFilters.getDefaultFilterNodes());
        if (object == null) {
            filters.addAll(siteFilters.getDefaultTypeFilterNodes());
        }
        if (object != null) {
            if (predicates.isEmpty()) {
                filters.add(new PathValue("_links", object));
            } else {
                filters.addAll(predicates.stream().map(p -> equalsLink(p, object)).toList());
            }
        }

        return filters;
    }
}
