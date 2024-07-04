package whelk.search2.querytree;

import whelk.search2.Operator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static whelk.search2.QueryUtil.shouldWrap;

public record Or(List<Node> children, Optional<String> nestedStem) implements Group {
    public Or(List<Node> children) {
        this(children, Optional.empty());
    }

    @Override
    public Group create(List<Node> children) {
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
                : Group.super.insertOperator(operator);
    }
}

