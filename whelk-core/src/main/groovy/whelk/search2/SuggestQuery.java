package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.And;
import whelk.search2.querytree.FreeText;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.QueryTreeBuilder;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static whelk.search2.QueryUtil.parenthesize;

public class SuggestQuery extends Query {
    private final QueryTree suggestQueryTree;

    public SuggestQuery(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, vocabMappings, esSettings, whelk);
        this.suggestQueryTree = getSuggestQueryTree();
    }

    @Override
    protected Object doGetEsQueryDsl() {
        applySiteFilters(suggestQueryTree, SearchMode.SUGGEST);
        return getEsQueryDsl(getEsQuery(suggestQueryTree, List.of()));
    }

    private QueryTree getSuggestQueryTree() throws InvalidQueryException {
        // TODO: Don't hardcode
        List<String> defaultBaseTypes = List.of("Agent", "Concept", "Language", "Work");
        return getSuggestQueryTree(defaultBaseTypes, queryTree, whelk.getJsonld(), disambiguate, queryParams.cursor);
    }

    private static QueryTree getSuggestQueryTree(List<String> defaultBaseTypes,
                                                 QueryTree qt,
                                                 JsonLd jsonLd,
                                                 Disambiguate disambiguate,
                                                 int cursorPos) throws InvalidQueryException {
        Optional<PathValue> currentlyEditedPathValue = qt.allDescendants()
                .filter(n -> n instanceof PathValue pv && pv.value() instanceof FreeText ft && ft.isEdited(cursorPos))
                .map(PathValue.class::cast)
                .findFirst();
        if (currentlyEditedPathValue.isPresent()) {
            PathValue pv = currentlyEditedPathValue.get();
            Optional<Property> property = pv.path().lastProperty();
            if (property.isPresent()) {
                Property p = property.get();
                String searchableTypes = p.range().stream()
                        .filter(type -> defaultBaseTypes.stream().anyMatch(baseType ->
                                baseType.equals(type) || jsonLd.getSubClasses(baseType).contains(type)))
                        .collect(Collectors.joining(" OR "));
                if (!searchableTypes.isEmpty()) {
                    Node typeFilter = QueryTreeBuilder.buildTree("\"rdf:type\":" + parenthesize(searchableTypes), disambiguate);
                    Node reverseLinksFilter = QueryTreeBuilder.buildTree("reverseLinks.totalItems>0", disambiguate);
                    return new QueryTree(new And(List.of((FreeText) pv.value(), typeFilter, reverseLinksFilter)));
                }
            }
        } else if (qt.isFreeText()) {
            String rawTypeFilter = "\"rdf:type\":" + parenthesize(String.join(" OR ", defaultBaseTypes));
            Node typeFilter = QueryTreeBuilder.buildTree(rawTypeFilter, disambiguate);
            return qt.addTopLevelNode(typeFilter);
        }
        return qt;
    }
}
