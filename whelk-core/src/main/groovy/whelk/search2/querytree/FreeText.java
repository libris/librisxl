package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.Operator;
import whelk.search2.QueryParams;
import whelk.util.Unicode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.shouldWrap;
import static whelk.search2.Operator.EQUALS;

public record FreeText(Property.TextQuery textQuery, Operator operator, String value, Collection<String> boostFields) implements Node {
    public FreeText(String value) {
        this(null, EQUALS, value, List.of());
    }

    public FreeText(Property.TextQuery textQuery, Operator operator, String value) { this(textQuery, operator, value, List.of()); }

    @Override
    // TODO: Review/refine this. So far it's basically just copy-pasted from old search code (EsQuery)
    public Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath) {
        String s = value;
        s = Unicode.normalizeForSearch(s);
        boolean isSimple = ESQuery.isSimple(s);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        if (!isSimple) {
            s = ESQuery.escapeNonSimpleQueryString(s);
        }
        Map<String, Object> simpleQuery = new HashMap<>();
        simpleQuery.put(queryMode,
                Map.of("query", s,
                        "analyze_wildcard", true,
                        "default_operator", "AND"
                )
        );

        if (boostFields.isEmpty()) {
            if (operator() == Operator.EQUALS) {
                return simpleQuery;
            }
            if (operator() == Operator.NOT_EQUALS) {
                return mustNotWrap(simpleQuery);
            }
        }

        List<String> softFields = boostFields.stream()
                .filter(f -> f.contains(JsonLd.SEARCH_KEY))
                .toList();
        List<String> exactFields = boostFields.stream()
                .map(f -> f.replace(JsonLd.SEARCH_KEY, JsonLd.SEARCH_KEY + ".exact"))
                .toList();

        var boostedExact = new HashMap<>();
        var be = Map.of(
                "query", s,
                "fields", exactFields,
                "analyze_wildcard", true,
                "default_operator", "AND"
        );
        boostedExact.put(queryMode, be);

        var boostedSoft = new HashMap<>();
        var bs = Map.of(
                "query", s,
                "fields", softFields,
                "quote_field_suffix", ".exact",
                "analyze_wildcard", true,
                "default_operator", "AND"
        );
        boostedSoft.put(queryMode, bs);

        var shouldClause = boostFields.contains("no-default-field") // dont search index.query.default_field, i.e. _all
            ? new ArrayList<>(List.of(boostedExact))
            : new ArrayList<>(Arrays.asList(boostedExact, boostedSoft, simpleQuery));

        if (operator() == Operator.EQUALS) {
            return shouldWrap(shouldClause);
        }
        if (operator() == Operator.NOT_EQUALS) {
                /*
                Better with { must: [must_not:{}, must_not:{}, must_not:{}] }?
                https://opster.com/guides/elasticsearch/search-apis/elasticsearch-query-bool/
                Limit the use of should clauses:
                While `should` clauses can be useful for boosting scores, they can also slow down your queries if used excessively.
                Try to limit the use of `should` clauses and only use them when necessary.
                 */
            return mustNotWrap(shouldWrap(shouldClause));
        }
        throw new RuntimeException("Invalid operator"); // Not reachable
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        return new FreeText(textQuery, operator, value, getBoostFields.apply(rulingTypes));
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("property", textQuery.definition());
        m.put(operator.termKey, value);
        m.put("up", makeUpLink(qt, this, queryParams));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return operator == Operator.NOT_EQUALS
                ? "NOT " + value :
                value;
    }

    @Override
    public Node getInverse() {
        return new FreeText(textQuery, operator.getInverse(), value);
    }

    @Override
    public boolean isFreeTextNode() {
        return operator.equals(EQUALS);
    }

    @Override
    public String toString() {
        return operator == Operator.NOT_EQUALS
                ? "NOT " + value :
                value;
    }

    public FreeText replace(String replacement) {
        return new FreeText(textQuery, operator, replacement, boostFields);
    }

    public boolean isWild() {
        return operator == EQUALS && Operator.WILDCARD.equals(value);
    }
}
