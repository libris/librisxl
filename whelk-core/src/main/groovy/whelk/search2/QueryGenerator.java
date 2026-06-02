package whelk.search2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.Condition;
import whelk.search2.querytree.FreeText;
import whelk.search2.querytree.Key;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Path;
import whelk.search2.querytree.Token;
import whelk.util.DocumentUtil;
import whelk.util.FresnelUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.REVERSE_KEY;
import static whelk.JsonLd.THING_KEY;
import static whelk.JsonLd.WORK_KEY;
import static whelk.JsonLd.asList;

public class QueryGenerator {
    private static final Logger log = LogManager.getLogger(QueryGenerator.class);

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
        try {
            _insertFindLinksOnBlankNodes(data, whelk);
        } catch (Exception e) {
            var id = DocumentUtil.getAtPath(data, List.of(JsonLd.ID_KEY));
            log.error("Failed to insert queries for {}", id, e);
        }
    }

    private static void _insertFindLinksOnBlankNodes(Object data, Whelk whelk) {
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

        var mainEntity = DocumentUtil.getAtPath(data, List.of(THING_KEY), null);
        if (mainEntity == null) {
            return;
        }

        DocumentUtil.traverse(mainEntity, (value, path) -> {
            if (path != null && path.size() > 1 && value instanceof Map<?,?> map) {
                var node = QueryUtil.castToStringObjectMap(map);
                if (node.containsKey(JsonLd.TYPE_KEY)) {
                    var t = node.get(JsonLd.TYPE_KEY);
                    if (t instanceof String type) {
                        if (type.equals("ISSN") && !"identifiedBy".equals(path.getFirst())) {
                            if (node.containsKey("value") && node.get("value") instanceof String issn) {
                                insert(new Condition(toKey("ISXN"), Operator.EQUALS, scopedFreeText(issn)), node);
                            }
                        }
                        else if (type.equals("Record") && path.contains("describedBy")) {
                            if (node.containsKey("controlNumber") && node.get("controlNumber") instanceof String controlNumber) {
                                insert(new Condition(toKey("controlNumber"), Operator.EQUALS, scopedFreeText(controlNumber)), node);
                            }
                        }
                        else if (type.equals("SeriesMembership")) {
                            if (node.containsKey("seriesStatement")) {
                                List<?> statements = asList(node.get("seriesStatement"));
                                node.put("seriesStatement", statements.stream()
                                        .map(statement -> {
                                            if (!(statement instanceof String s)) {
                                                return statement;
                                            }
                                            
                                            var v  = new HashMap<String, Object>();
                                            v.put(JsonLd.TYPE_KEY, "_Value");
                                            v.put("label", s);
                                            insert(
                                                    new Condition(toKey("seriesMembership"), Operator.EQUALS, scopedFreeText(s)),
                                                    QueryUtil.castToStringObjectMap(v)
                                            );
                                            return v;
                                        }).toList());
                            }
                        }
                        else if (type.equals("Title") && path.contains("inSeries") && node.containsKey("mainTitle")) {
                            var title = String.valueOf(node.get("mainTitle"));
                            insert(
                                    new Condition(toKey("seriesMembership"), Operator.EQUALS, scopedFreeText(title)),
                                    node
                            );
                        }
                    }
                }
            }

            return DocumentUtil.NOP;
        });
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

        insert(q, node);
    }

    private static void insert(Node query, Map<String, Object> node) {
        try {
            var url = QueryUtil.makeViewFindUrl(query.toQueryString(true), new QueryParams(Collections.emptyMap()));
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
                .map(p -> toKey(String.valueOf(p))).toList());
    }

    private static Key.UnrecognizedKey toKey(String s) {
        return new Key.UnrecognizedKey(new Token.Raw(s, 0));
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
