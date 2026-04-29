package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.Condition;
import whelk.search2.querytree.FreeText;
import whelk.search2.querytree.Key;
import whelk.search2.querytree.Path;
import whelk.search2.querytree.Selector;
import whelk.search2.querytree.Token;
import whelk.util.DocumentUtil;
import whelk.util.FresnelUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.REVERSE_KEY;
import static whelk.JsonLd.THING_KEY;
import static whelk.JsonLd.WORK_KEY;

public class QueryGenerator {

    private static final List<List<Object>> PATHS = List.of(
            List.of(THING_KEY, "classification", "*"),
            List.of(THING_KEY, WORK_KEY, "classification", "*"),
            List.of(THING_KEY, "publication", "*", "agent"),
            List.of(THING_KEY, REVERSE_KEY, WORK_KEY, "*", "publication", "*", "agent")
    );

    /*
    TODO
    We will have to revisit how we generate queries.
    This is just to have something to test in the frontend
     */
    public static void insertFindLinksOnBlankNodes(Object data, Whelk whelk) {
        for (var path : PATHS) {
            var nodes = DocumentUtil.getAtPath(data, path, Collections.emptyList());

            if (nodes instanceof List<?> l) {
                for (var value : l) {
                    if (value instanceof Map<?, ?> map) {
                        var node = QueryUtil.castToStringObjectMap(map);
                        if (isBlank(node)) {
                            insert(path, node, whelk);
                        }
                    }
                }
            }
        }
    }

    private static void insert(List<Object> path, Map<String, Object> node, Whelk whelk) {
        var qPath = toQueryPath(path, whelk.jsonld);

        var text = whelk.getFresnelUtil().asString(node, FresnelUtil.Lenses.SEARCH_NEEDLE);
        if (text.isBlank()) {
            return;
        }

        // TODO
        var q = qPath.path().isEmpty()
                ? new FreeText(text)
                : new Condition(qPath, Operator.EQUALS, scopedFreeText(text));

        try {
            var url = QueryUtil.makeViewFindUrl(q.toQueryString(true), new QueryParams(Collections.emptyMap()));
            node.put("_find", Map.of(
                    JsonLd.TYPE_KEY, "_Query",
                    JsonLd.ID_KEY, url
            ));
        } catch (InvalidQueryException e) {
            throw new RuntimeException(e);
        }
    }

    private static Path toQueryPath(List<Object> docPath, JsonLd jsonLd) {
        return new Path(docPath.stream()
                .filter(s -> !THING_KEY.equals(s))
                .filter(s -> !(s instanceof Number))
                .filter(s -> !"*".equals(s))
                .filter(s -> !JsonLd.REVERSE_KEY.equals(s))
                .filter(s -> !jsonLd.isIntegral(String.valueOf(s)))
                .map(p -> new Key.UnrecognizedKey(new Token.Raw(String.valueOf(p), 0))).toList());
    }

    private static FreeText scopedFreeText(String s) {
        var tokens = Arrays.stream(s.split("\\s")).map(t -> (Token) new Token.Raw(t)).toList();
        return new FreeText(null, tokens, Query.Connective.AND);
    }

    private static boolean isBlank(Map<String, Object> node) {
        return !node.containsKey(JsonLd.ID_KEY)
                && node.containsKey(JsonLd.TYPE_KEY);
    }
}
