package whelk.util;

import whelk.search2.querytree.Property;
import whelk.search2.querytree.Value;

import java.util.Collection;
import java.util.Map;


public class Restrictions {
    public static String CATEGORY = "category";
    public static String FIND_CATEGORY = "librissearch:findCategory";
    public static String IDENTIFY_CATEGORY = "librissearch:identifyCategory";
    public static String NONE_CATEGORY = "librissearch:noneCategory";

    public sealed interface OnProperty permits HasNoneOfValues, HasValue {
    }

    public record HasValue(Property property, Value value) implements OnProperty {
    }

    public record HasNoneOfValues(Property property, Collection<Value> values) implements OnProperty {
    }

    public static boolean isNarrowingProperty(String propertyName) {
        return NARROWS.containsKey(propertyName);
    }

    public static final Map<String, String> NARROWS = Map.of(
            FIND_CATEGORY, CATEGORY,
            IDENTIFY_CATEGORY, CATEGORY,
            NONE_CATEGORY, CATEGORY
    );
}
