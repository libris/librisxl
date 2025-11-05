package whelk.search2;

import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;
import static whelk.search2.QueryUtil.parenthesize;

public class SuggestQuery extends Query {
    // TODO: Don't hardcode
    private static final List<String> defaultBaseTypes = List.of("Agent", "Concept", "Language", "Work");

    private static final Map<String, List<String>> suggestPredicatesForType = new LinkedHashMap<>() {{
        put("Bibliography", List.of("bibliography"));
        put("Library", List.of("itemHeldBy"));
        put("Subject", List.of("subject"));
        put("GenreForm", List.of("category"));
        put("Language", List.of("language", "originalLanguage"));
        put("BibliographicAgent", List.of("contributor", "subject"));
    }};

    private record Edited(Node node, Token token) {
        static Edited empty() {
            return new Edited(null, null);
        }
    }

    private final Edited edited;
    private final QueryTree suggestQueryTree;

    private boolean propertySearch = false;

    public SuggestQuery(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, vocabMappings, esSettings, whelk);
        this.edited = getEdited();
        this.suggestQueryTree = getSuggestQueryTree();
    }

    @Override
    protected Map<String, Object> getPartialCollectionView() {
        Map<String, Object> view = super.getPartialCollectionView();

        Map<String, Property> propertyByKey = new HashMap<>();

        var suggestions = ((List<?>) view.get("items")).stream()
                .map(Map.class::cast)
                .peek(item -> {
                    var qualifiers = getApplicablePredicates(item, propertyByKey).stream()
                            .map(predicate -> {
                                var predicateDefinition = predicate.lastProperty().map(Property::definition).orElse(Map.of());// TODO: Handle property paths?
                                String formattedLink = new Link((String) item.get(ID_KEY)).queryForm();
                                Link placeholderLink = new Link("http://PLACEHOLDER_LINK");
                                PathValue placeholderNode = new PathValue(predicate, Operator.EQUALS, placeholderLink);
                                String template = qTree.replace(edited.node(), placeholderNode).toQueryString();
                                int placeholderLinkStart = template.indexOf(placeholderLink.queryForm());
                                int placeholderLinkEnd = placeholderLinkStart + placeholderLink.queryForm().length();
                                String q = template.substring(0, placeholderLinkStart) + formattedLink + template.substring(placeholderLinkEnd);
                                int newCursorPos = placeholderLinkStart + formattedLink.length();
                                if (newCursorPos == q.length()) {
                                    q += " ";
                                    newCursorPos += 1;
                                }
                                return Map.of("_predicate", predicateDefinition,
                                        "_q", QueryUtil.makeViewFindUrl(q, queryParams),
                                        "_cursor", newCursorPos);
                            })
                            .toList();
                    item.put("_qualifiers", qualifiers);
                })
                .toList();

        view.put("items", suggestions);

        return view;
    }

    @Override
    protected Object doGetEsQueryDsl() {
        var queryDsl = getEsQueryDsl(getEsQuery(getFullQueryTree(suggestQueryTree)));
        queryDsl.remove("sort");
        return queryDsl;
    }

    private List<Path> getApplicablePredicates(Map<?, ?> item, Map<String, Property> propertyByKey) {
        List<Path> applicablePredicates = new ArrayList<>();
        if (edited.node() instanceof PathValue editedPv && propertySearch) {
            applicablePredicates.add(editedPv.path());
        } else if (edited.node() instanceof FreeText) {
            var types = asList(item.get(TYPE_KEY));
            applicablePredicates = suggestPredicatesForType.entrySet()
                    .stream()
                    .filter(e -> types.contains(e.getKey())
                            || whelk.getJsonld().getSubClasses(e.getKey()).stream().anyMatch(types::contains))
                    .findFirst()
                    .map(Map.Entry::getValue)
                    .map(predicates -> predicates.stream()
                            .map(p -> propertyByKey.computeIfAbsent(p, x -> new Property(p, whelk.getJsonld())))
                            .map(Path::new)
                            .toList())
                    .orElse(List.of());
        }
        return applicablePredicates;
    }

    private Edited getEdited() {
        return qTree.allDescendants().flatMap(node ->
                        switch (node) {
                            case FreeText ft -> ft.getCurrentlyEditedToken(queryParams.cursor)
                                    .map(token -> new Edited(ft, token))
                                    .stream();
                            case PathValue pv -> pv.value() instanceof FreeText ft
                                    ? ft.getCurrentlyEditedToken(queryParams.cursor)
                                    .map(token -> new Edited(node, token))
                                    .stream()
                                    : Stream.empty();
                            default -> Stream.empty();
                        })
                .findFirst()
                .orElse(Edited.empty());
    }

    private QueryTree getSuggestQueryTree() throws InvalidQueryException {
        if (edited.node() instanceof PathValue pv && pv.operator().equals(Operator.EQUALS)) {
            Optional<Property> property = pv.path().lastProperty();
            if (property.isPresent()) {
                Property p = property.get();
                String searchableTypes = p.range().stream()
                        .filter(type -> defaultBaseTypes.stream()
                                .filter(Predicate.not("Work"::equals))
                                .anyMatch(baseType -> baseType.equals(type) || whelk.getJsonld().getSubClasses(baseType).contains(type)))
                        .collect(Collectors.joining(" OR "));

                FreeText ft = (FreeText) pv.value();

                FreeText prefixFt = editedTokenAsPrefix(ft);

                if (searchableTypes.isEmpty()) {
                    // Make a query with the currently edited token treated as prefix to get suggestions
                    // Also make a non-prefix query to get a higher relevancy score for exact matches
                    Or or = new Or(List.of(pv.withValue(prefixFt), pv));
                    return qTree.replace(pv, or);
                }

                this.propertySearch = true;
                Or or = new Or(List.of(prefixFt, ft));
                Node typeFilter = QueryTreeBuilder.buildTree("\"rdf:type\":" + parenthesize(searchableTypes), disambiguate);
                Node reverseLinksFilter = QueryTreeBuilder.buildTree("reverseLinks.totalItems>0", disambiguate);
                return new QueryTree(new And(List.of(or, typeFilter, reverseLinksFilter)));
            }
        } else if (edited.node() instanceof FreeText ft && qTree.isSimpleFreeText()) {
            String rawTypeFilter = "\"rdf:type\":" + parenthesize(String.join(" OR ", defaultBaseTypes));
            Node typeFilter = QueryTreeBuilder.buildTree(rawTypeFilter, disambiguate);
            return (edited.token().isQuoted() ? qTree : qTree.replace(ft, new Or(List.of(editedTokenAsPrefix(ft), ft))))
                    .add(typeFilter);
        }
        return qTree;
    }

    private FreeText editedTokenAsPrefix(FreeText ft) {
        List<Token> tokensCopy = new ArrayList<>(ft.tokens());
        int editedIdx = ft.tokens().indexOf(edited.token());
        Token editedAsPrefix = new Token.Raw(edited.token().value() + Operator.WILDCARD);
        tokensCopy.set(editedIdx, editedAsPrefix);
        return ft.withTokens(tokensCopy);
    }
}
