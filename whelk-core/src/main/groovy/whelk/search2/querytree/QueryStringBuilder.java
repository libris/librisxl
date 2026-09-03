package whelk.search2.querytree;

import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.FilterAlias;
import whelk.search2.querytree.value.FreeText;
import whelk.search2.querytree.node.Group;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Not;

import java.util.stream.Collectors;

import static whelk.search2.Query.Connective.OR;
import static whelk.search2.QueryUtil.parenthesize;

public class QueryStringBuilder {
    public static String buildString(Node tree, boolean topLevel) {
        return switch (tree) {
            case Condition condition -> buildFromCondition(condition, topLevel);
            case FilterAlias filterAlias -> filterAlias.alias();
            case Group group -> buildFromGroup(group, topLevel);
            case Not not -> buildFromNot(not);
        };
    }

    private static String buildFromCondition(Condition condition, boolean topLevel) {
        if (condition.isAnyQuery()) {
            return condition.value().queryForm();
        }
        if (condition.isTextQuery()) {
            return buildFromFreeTextValue(condition.freeTextValue(), topLevel);
        }

        String k = condition.selector().formattedQueryKey();
        String v = condition.value().queryForm();
        if (condition.value().isMultiToken()) {
            v = parenthesize(v);
        }

        return condition.operator().format(k, v);
    }

    private static String buildFromGroup(Group group, boolean topLevel) {
        String s = group.children().stream()
                .map(child -> buildString(child, false))
                .collect(Collectors.joining(group.delimiter()));
        return topLevel ? s : parenthesize(s);
    }

    private static String buildFromNot(Not not) {
        String s = not.node() instanceof Condition c
                && c.isTextQuery()
                && c.freeTextValue().isMultiToken()
                ? parenthesize(buildFromFreeTextValue(c.freeTextValue(), true))
                : buildString(not.node(), false);
        return "NOT " + s;
    }

    private static String buildFromFreeTextValue(FreeText freeText, boolean topLevel) {
        String s = freeText.queryForm();
        if (freeText.isMultiToken() && !topLevel && freeText.connective() == OR) {
            s = parenthesize(s);
        }
        return s;
    }
}
