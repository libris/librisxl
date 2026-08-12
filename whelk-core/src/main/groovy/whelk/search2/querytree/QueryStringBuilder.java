package whelk.search2.querytree;

import java.util.stream.Collectors;

import static whelk.search2.Query.Connective.OR;
import static whelk.search2.QueryUtil.parenthesize;

public class QueryStringBuilder {
    public static String buildFrom(Node tree, boolean topLevel) {
        return switch (tree) {
            case Any any -> any.queryForm();
            case Condition condition -> buildFromCondition(condition);
            case FilterAlias filterAlias -> filterAlias.alias();
            case FreeText freeText -> buildFromFreeText(freeText, topLevel);
            case Group group -> buildFromGroup(group, topLevel);
            case Not not -> buildFromNot(not);
        };
    }

    private static String buildFromCondition(Condition condition) {
        String k = condition.selector().formattedQueryKey();
        String v = condition.value().queryForm();
        if (condition.value().isMultiToken()) {
            v = parenthesize(v);
        }
        return condition.operator().format(k, v);
    }

    private static String buildFromFreeText(FreeText freeText, boolean topLevel) {
        String s = freeText.queryForm();
        if (freeText.isMultiToken() && !topLevel && freeText.connective() == OR) {
            s = parenthesize(s);
        }
        return s;
    }

    private static String buildFromGroup(Group group, boolean topLevel) {
        String s = group.children().stream()
                .map(child -> buildFrom(child, false))
                .collect(Collectors.joining(group.delimiter()));
        return topLevel ? s : parenthesize(s);
    }

    private static String buildFromNot(Not not) {
        String s = not.node() instanceof FreeText ft && ft.isMultiToken()
                ? parenthesize(buildFromFreeText(ft, true))
                : buildFrom(not.node(), false);
        return "NOT " + s;
    }
}
