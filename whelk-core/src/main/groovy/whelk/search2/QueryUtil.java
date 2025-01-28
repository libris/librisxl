package whelk.search2;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.search2.querytree.QueryTree;
import whelk.util.DocumentUtil;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.component.ElasticSearch.flattenedLangMapKey;

public class QueryUtil {
    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper();

    private final Whelk whelk;
    public final EsMappings esMappings;
    public final EsBoost esBoost;

    public QueryUtil(Whelk whelk) {
        this.whelk = whelk;
        this.esMappings = new EsMappings(whelk.elastic != null ? whelk.elastic.getMappings() : Collections.emptyMap());
        this.esBoost = new EsBoost(whelk.getJsonld());
    }

    public Map<?, ?> query(Map<String, Object> queryDsl) {
        return whelk.elastic.query(queryDsl);
    }

    public boolean esIsConfigured() {
        return whelk != null && whelk.elastic != null;
    }

    public int maxItems() {
        return whelk.elastic.maxResultWindow;
    }

    public static String quoteIfPhraseOrContainsSpecialSymbol(String s) {
        // TODO: Don't hardcode
        return s.matches(".*(>=|<=|[=!~<>(): ]).*") ? "\"" + s + "\"" : s;
    }

    public static String encodeUri(String uri) {
        String decoded = URLDecoder.decode(uri.replace("+", "%2B"), StandardCharsets.UTF_8);
        return escapeQueryParam(decoded)
                .replace("%23", "#")
                .replace("+", "%20");
    }

    public static Map<String, Object> castToStringObjectMap(Object o) {
        return ((Map<?, ?>) o).entrySet()
                .stream()
                .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (Object) e.getValue()));
    }

    public Optional<Map<?, ?>> loadThing(String id) {
        return loadThing(id, whelk);
    }

    public static Optional<Map<?, ?>> loadThing(String id, Whelk whelk) {
        return Optional.ofNullable(whelk.loadData(id))
                .map(data -> data.get(JsonLd.GRAPH_KEY))
                .map(graph -> (Map<?, ?>) ((List<?>) graph).get(1));
    }

    public static String makeFindUrl(String i, String q, Map<String, String> nonQueryParams) {
        return makeFindUrl(i, q, makeParams(nonQueryParams));
    }

    public static String makeFindUrl(QueryTree qt, Map<String, String> nonQueryParams) {
        return makeFindUrl(qt.getTopLevelFreeText(), qt.toString(), nonQueryParams);
    }

    public static String makeFindUrl(String i, String q, List<String> nonQueryParams) {
        List<String> params = new ArrayList<>();
        params.add(makeParam(QueryParams.ApiParams.SIMPLE_FREETEXT, i));
        params.add(makeParam(QueryParams.ApiParams.QUERY, q));
        params.addAll(nonQueryParams);
        return makeFindUrl(params);
    }

    public static String makeFindUrl(List<String> params) {
        return "/find?" + String.join("&", params);
    }

    public static List<String> makeParams(Map<String, String> params) {
        return params.entrySet()
                .stream()
                .map(entry -> makeParam(entry.getKey(), entry.getValue()))
                .toList();
    }

    private static String makeParam(String key, String value) {
        return String.format("%s=%s", escapeQueryParam(key), escapeQueryParam(value));
    }

    private static String escapeQueryParam(String input) {
        return QUERY_ESCAPER.escape(input)
                // We want pretty URIs, restore some characters which are inside query strings
                // https://tools.ietf.org/html/rfc3986#section-3.4
                .replace("%3A", ":")
                .replace("%2F", "/")
                .replace("%40", "@");
    }

    public Optional<String> getNestedPath(String path) {
        if (esMappings.isNestedField(path)) {
            return Optional.of(path);
        }
        return esMappings.getNestedFields().stream().filter(path::startsWith).findFirst();
    }

    public static Map<String, Object> mustWrap(Object l) {
        return boolWrap(Map.of("must", l));
    }

    public static Map<String, Object> mustNotWrap(Object o) {
        return boolWrap(Map.of("must_not", o));
    }

    public static Map<String, Object> shouldWrap(List<?> l) {
        return boolWrap(Map.of("should", l));
    }

    public static Map<String, Object> boolWrap(Map<?, ?> m) {
        return Map.of("bool", m);
    }

    public static Map<String, Object> nestedWrap(String nestedPath, Map<String, Object> query) {
        return Map.of("nested", Map.of("path", nestedPath, "query", query));
    }

    public String getSortField(String termPath) {
        var path = expandLangMapKeys(termPath);
        if (esMappings.isKeywordField(path) && !esMappings.isFourDigitField(path)) {
            return String.format("%s.keyword", path);
        } else {
            return termPath;
        }
    }

    private String expandLangMapKeys(String field) {
        var parts = field.split("\\.");
        if (parts.length > 0) {
            assert whelk != null;
            var lastIx = parts.length - 1;
            if (whelk.getJsonld().langContainerAlias.containsKey(parts[lastIx])) {
                parts[lastIx] = flattenedLangMapKey(parts[lastIx]);
                return String.join(".", parts);
            }
        }
        return field;
    }

    @SuppressWarnings("unchecked")
    public Function<Map<String, Object>, Map<String, Object>> getApplyLensFunc(QueryParams queryParams) {
        return framedThing -> {
            @SuppressWarnings("rawtypes")
            Set<String> preserveLinks = Stream.ofNullable(queryParams.object).collect(Collectors.toSet());

            return switch (queryParams.lens) {
                case "chips" -> (Map<String, Object>) whelk.getJsonld().toChip(framedThing, preserveLinks);
                case "full" -> removeSystemInternalProperties(framedThing);
                default -> whelk.getJsonld().toCard(framedThing, false, false, false, preserveLinks, true);
            };
        };
    }

    private static Map<String, Object> removeSystemInternalProperties(Map<String, Object> framedThing) {
        DocumentUtil.traverse(framedThing, (value, path) -> {
            if (!path.isEmpty() && ((String) path.getLast()).startsWith("_")) {
                return new DocumentUtil.Remove();
            }
            return DocumentUtil.NOP;
        });
        return framedThing;
    }
}
