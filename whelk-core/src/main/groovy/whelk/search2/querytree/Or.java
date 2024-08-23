package whelk.search2.querytree;

import whelk.search2.Operator;

import java.util.List;
import java.util.Map;

import static whelk.search2.QueryUtil.shouldWrap;

public final class Or extends Group {
    private final List<Node> children;

    public Or(List<Node> children) {
        this.children = flattenChildren(children);
    }

    @Override
    public List<Node> children() {
        return children;
    }

    @Override
    public Group newInstance(List<Node> children) {
        return new Or(children);
    }

    @Override
    public String delimiter() {
        return " OR ";
    }

    @Override
    public String key() {
        return "or";
    }

    @Override
    public Map<String, Object> wrap(List<Map<String, Object>> esChildren) {
        return shouldWrap(esChildren);
    }

    @Override
    public Group insertOperator(Operator operator) {
        return operator == Operator.NOT_EQUALS
                ? new And(children).insertOperator(operator)
                : super.insertOperator(operator);
    }
}

