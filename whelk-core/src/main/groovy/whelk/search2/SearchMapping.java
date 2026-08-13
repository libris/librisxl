package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.FilterAlias;
import whelk.search2.querytree.node.Group;
import whelk.search2.querytree.value.Link;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Not;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.value.Resource;
import whelk.search2.querytree.selector.Selector;
import whelk.search2.querytree.value.Value;

import java.util.LinkedHashMap;
import java.util.Map;

import static whelk.search2.Operator.EQUALS;
import static whelk.search2.Operator.LIKE;
import static whelk.search2.QueryUtil.makeViewFindUrl;

public class SearchMapping {
    public static Map<String, Object> buildFrom(QueryTree queryTree, QueryParams queryParams, String apiParam) {
        return buildFrom(queryTree.tree(), queryTree, queryParams, apiParam);
    }

    private static Map<String, Object> buildFrom(Node node, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        return switch (node) {
            case Condition c -> buildFromCondition(c, queryTree, queryParams, apiParam);
            case FilterAlias fa -> buildFromFilterAlias(fa, queryTree, queryParams, apiParam);
            case Group g -> buildFromGroup(g, queryTree, queryParams, apiParam);
            case Not n -> buildFromNot(n, queryTree, queryParams, apiParam);
        };
    }

    private static Map<String, Object> buildFromCondition(Condition condition, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        Selector s = condition.selector();
        Operator op = condition.operator();
        Value v = condition.value();

        Map<String, Object> m = new LinkedHashMap<>();

        m.put("property", s.definition());
        m.put(op.termKey, v instanceof Resource r ? r.description() : v.queryForm());
        m.put("up", makeUpLink(condition, queryTree, queryParams, apiParam));

        if (op == LIKE) {
            m.put("toEquals", makeReplaceLink(condition, condition.withOperator(EQUALS), queryTree, queryParams, apiParam));
        }
        if (op == EQUALS && s instanceof Property p && v instanceof Link && p.isPreferLike()) {
            m.put("toLike", makeReplaceLink(condition, condition.withOperator(LIKE), queryTree, queryParams, apiParam));
        }

        if (!(s instanceof Property.TextQuery || s instanceof Property.AnyQuery)) {
            m.put("_key", s.queryKey());
            m.put("_value", v.queryForm());
        }

        return m;
    }

    private static Map<String, Object> buildFromFilterAlias(FilterAlias fa, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        Map<String, Object> m = new LinkedHashMap<>();
        LinkedHashMap<String, Object> description = new LinkedHashMap<>(fa.description());
        description.put("parsedFilter", buildFrom(fa.getParsed(), queryTree, queryParams, apiParam));
        m.put("object", description);
        m.put("value", fa.alias());
        m.put("up", makeUpLink(fa, queryTree, queryParams, apiParam));
        return m;
    }

    private static Map<String, Object> buildFromGroup(Group group, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(group.key(), group.children().stream().map(c -> buildFrom(c, queryTree, queryParams, apiParam)).toList());
        m.put("up", makeUpLink(group, queryTree, queryParams, apiParam));
        return m;
    }

    private static Map<String, Object> buildFromNot(Not not, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("not", buildFrom(not.node(), queryTree, queryParams, apiParam));
        m.put("up", makeUpLink(not, queryTree, queryParams, apiParam));
        return m;
    }

    private static Map<String, String> makeUpLink(Node node, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        return makeFindLink(queryTree.remove(node), queryParams, apiParam);
    }

    private static Map<String, String> makeReplaceLink(Node node, Node replacement, QueryTree queryTree, QueryParams queryParams, String apiParam) {
        return makeFindLink(queryTree.replace(node, replacement), queryParams, apiParam);
    }

    private static Map<String, String> makeFindLink(QueryTree queryTree, QueryParams queryParams, String apiParam) {
        String findUrl = makeViewFindUrl(queryTree.toQueryString(), queryParams, apiParam);
        return Map.of(JsonLd.ID_KEY, findUrl);
    }
}