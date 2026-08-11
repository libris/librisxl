package whelk.search2.querytree;

import whelk.search2.QueryUtil;

import java.util.*;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public sealed abstract class Group implements Node permits And, Or {
    abstract Group newInstance(List<Node> children, boolean flattenChildren);

    abstract String delimiter();

    abstract String key();

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink, BiFunction<Node, Node, Map<String, String>> makeReplaceLink) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key(), children().stream().map(c -> c.toSearchMapping(makeUpLink, makeReplaceLink)).toList());
        m.put("up", makeUpLink.apply(this));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        String s = children().stream().map(n -> n.toQueryString(false))
                .collect(Collectors.joining(delimiter()));
        return topLevel ? s : QueryUtil.parenthesize(s);
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), new HashSet<>(children()));
    }

    List<Node> flattenChildren(List<? extends Node> children) {
        List<Node> flattened = new ArrayList<>();
        for (Node child : children) {
            if (child instanceof Group g && g.getClass() == this.getClass()) {
                g.children().stream().filter(c -> !flattened.contains(c) && !children.contains(c)).forEach(flattened::add);
            } else {
                flattened.add(child);
            }
        }
        return flattened;
    }
}
