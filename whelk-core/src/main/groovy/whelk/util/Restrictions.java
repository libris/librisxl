package whelk.util;

import whelk.search2.querytree.Property;
import whelk.search2.querytree.Value;

public class Restrictions {
    public sealed interface OnProperty permits HasValue {
    }

    public record HasValue(Property onProperty, Value value) implements OnProperty {
    }
}
