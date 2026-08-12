package whelk.search2.querytree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public sealed abstract class Group implements Node permits And, Or {
    abstract Group newInstance(List<Node> children, boolean flattenChildren);

    public abstract String delimiter();

    public abstract String key();

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), new HashSet<>(children()));
    }

    @Override
    public String toString() {
        return toQueryString();
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
