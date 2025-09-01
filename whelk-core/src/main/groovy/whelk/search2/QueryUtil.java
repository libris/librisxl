package whelk.search2;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.QueryTree;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static whelk.search2.QueryParams.ApiParams.OBJECT;
import static whelk.search2.QueryParams.ApiParams.OFFSET;
import static whelk.search2.QueryParams.ApiParams.PREDICATES;
import static whelk.search2.QueryParams.ApiParams.QUERY;
import static whelk.search2.QueryParams.ApiParams.SORT;

public class QueryUtil {
    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper();

    public static String quote(String s) {
        return "\"" + s + "\"";
    }

    public static boolean isQuoted(String s) {
        return s.matches("\".+\"");
    }

    public static String parenthesize(String s) {
        return "(" + s + ")";
    }

    public static String encodeUri(String uri) {
        String decoded = URLDecoder.decode(uri.replace("+", "%2B"), StandardCharsets.UTF_8);
        return escapeQueryParam(decoded)
                .replace("%23", "#")
                .replace("+", "%20");
    }

    public static Map<String, Object> castToStringObjectMap(Object o) {
        Map<String, Object> m = new HashMap<>();
        if (o == null) {
            return m;
        }
        ((Map<?, ?>) o).forEach((k, v) -> m.put((String) k, v));
        return m;
    }

    public static String makeViewFindUrl(QueryTree qt, QueryParams queryParams) {
        return makeViewFindUrl(qt.toQueryString(), queryParams);
    }

    public static String makeViewFindUrl(String q, QueryParams queryParams) {
        return makeFindUrl(q, queryParams.getCustomParamsMap(List.of(SORT, OBJECT, PREDICATES)), null);
    }

    public static String makeFindUrl(QueryTree qt, QueryParams queryParams) {
        return makeFindUrl(qt.toQueryString(), queryParams.getFullParamsMap(), null);
    }

    public static String makeFindUrl(QueryTree qt, QueryParams queryParams, Integer customOffset) {
        return makeFindUrl(qt.toQueryString(), queryParams.getFullParamsMap(), customOffset);
    }

    private static String makeFindUrl(String q, Map<String, String> params, Integer customOffset) {
        params.put(QUERY, q);
        if (customOffset != null) {
            if (customOffset > 0) {
                params.put(OFFSET, "" + customOffset);
            } else {
                params.remove(OFFSET);
            }
        }
        return makeFindUrl(params);
    }

    public static String makeFindUrl(Map<String, String> params) {
        return "/find?" + params.entrySet().stream()
                .map(e -> makeParam(e.getKey(), e.getValue()))
                .collect(Collectors.joining("&"));
    }

    public static Map<String, String> makeUpLink(QueryTree queryTree, Node n, QueryParams queryParams) {
        QueryTree reducedTree = queryTree.remove(n);
        String upUrl = makeViewFindUrl(reducedTree, queryParams);
        return Map.of(JsonLd.ID_KEY, upUrl);
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

    public static Map<String, Object> loadThing(String iri, Whelk whelk) {
        return Optional.ofNullable(whelk.loadData(iri))
                .map(data -> data.get(JsonLd.GRAPH_KEY))
                .map(graph -> ((List<?>) graph).get(1))
                .map(QueryUtil::castToStringObjectMap)
                .orElse(Collections.emptyMap());
    }

    // leading wildcards e.g. "*foo" are removed by simple_query_string
    static Pattern NON_SIMPLE_QUERY = Pattern.compile("\\\\[?]|([*?])\\S+");

    /**
     * Can this query string be handled by ES simple_query_string?
     */
    public static boolean isSimple(String queryString) {
        return !NON_SIMPLE_QUERY.matcher(queryString).find();
    }

    public static String escapeNonSimpleQueryString(String queryString) {
        // Treat escaped question marks as actual wildcards
        queryString = queryString.replace("\\?", "?");

        // The following chars are reserved in ES and need to be escaped to be used as literals: \+-=|&><!(){}[]^"~*?:/
        // Escape the ones that are not part of our query language.
        for (char c : List.of('=', '&', '!', '{', '}', '[', ']', '^', ':', '/')) {
            queryString = queryString.replace("" + c, "\\" + c);
        }

        // Inside words, treat '-' as regular hyphen instead of "NOT" and escape it
        queryString = queryString.replaceAll("(^|\\s+)-(\\S+)", "$1#ACTUAL_NOT#$2");
        queryString = queryString.replace("-", "\\-");
        queryString = queryString.replace("#ACTUAL_NOT#", "-");

        // Strip un-escapable characters
        for (char c : List.of('<', '>')) {
            queryString = queryString.replace("" + c, "");
        }

        return queryString;
    }
}
