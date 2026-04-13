package whelk.search2;

import whelk.util.DocumentUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static whelk.util.DocumentUtil.NOP;

public class EsMappings {
    private final Set<String> keywordSubfieldFields;

    private final Set<String> fourDigitsKeywordFields;
    private final Set<String> fourDigitsShortFields;

    private final Set<String> keywordTypeFields;
    private final Set<String> dateTypeFields;
    private final Set<String> nestedTypeFields;
    private final Set<String> longTypeFields;

    private final Set<String> nestedNotInParentFields;

    public static String KEYWORD = "keyword";
    public static String FOUR_DIGITS_KEYWORD_SUFFIX = "_4_digits_keyword";
    public static String FOUR_DIGITS_SHORT_SUFFIX = "_4_digits_short";

    public EsMappings(List<Map<?, ?>> mappings) {
        this.keywordSubfieldFields = union(mappings, EsMappings::getKeywordSubfieldFields);
        this.fourDigitsKeywordFields = union(mappings, EsMappings::getFourDigitsKeywordFields);
        this.fourDigitsShortFields = union(mappings, EsMappings::getFourDigitsShortFields);
        this.keywordTypeFields = union(mappings, m -> getFieldsOfType("keyword", m));
        this.dateTypeFields = union(mappings, m -> getFieldsOfType("date", m));
        this.nestedTypeFields = union(mappings, m -> getFieldsOfType("nested", m));
        this.longTypeFields = union(mappings, m -> getFieldsOfType("long", m));
        this.nestedNotInParentFields = new HashSet<>(nestedTypeFields);
        var includeInParent = union(mappings, m -> getFieldsWithSetting("include_in_parent", true, m));
        this.nestedNotInParentFields.removeAll(includeInParent);
    }

    public boolean hasKeywordSubfield(String fieldPath) {
        return keywordSubfieldFields.contains(fieldPath);
    }

    public boolean hasFourDigitsKeywordField(String fieldPath) {
        return fourDigitsKeywordFields.contains(fieldPath + FOUR_DIGITS_KEYWORD_SUFFIX);
    }

    public boolean hasFourDigitsShortField(String fieldPath) {
        return fourDigitsShortFields.contains(fieldPath + FOUR_DIGITS_SHORT_SUFFIX);
    }

    public boolean isDateTypeField(String fieldPath) {
        return dateTypeFields.contains(fieldPath);
    }

    public boolean isNestedTypeField(String fieldPath) {
        return nestedTypeFields.contains(fieldPath);
    }

    public boolean isLongTypeField(String fieldPath) {
        return longTypeFields.contains(fieldPath);
    }

    public boolean isKeywordTypeField(String fieldPath) {
        return keywordTypeFields.contains(fieldPath);
    }

    public boolean isNestedNotInParentField(String fieldPath) {
        return nestedNotInParentFields.contains(fieldPath);
    }

    public boolean isAggregatable(String fieldPath) {
        // Simple check based on current mappings.
        // Doesn't consider
        // - text fields with `"fielddata": true` (aggregatable)
        // - fields with "doc_values": false` (not aggregatable)
        // This is fine since the current index settings do not use these options.
        return keywordTypeFields.contains(fieldPath)
                || dateTypeFields.contains(fieldPath)
                || longTypeFields.contains(fieldPath);
    }

    public Set<String> getNestedTypeFields() {
        return nestedTypeFields;
    }

    private static Set<String> getFourDigitsKeywordFields(Map<?, ?> mappings) {
        return getFieldsWithSuffix(mappings, FOUR_DIGITS_KEYWORD_SUFFIX);
    }

    private static Set<String> getFourDigitsShortFields(Map<?, ?> mappings) {
        return getFieldsWithSuffix(mappings, FOUR_DIGITS_SHORT_SUFFIX);
    }

    private static Set<String> getKeywordSubfieldFields(Map<?, ?> mappings) {
        return getFieldsWithSetting("fields", v -> v instanceof Map<?, ?> m && m.containsKey(KEYWORD), mappings);
    }

    private static Set<String> getFieldsOfType(String type, Map<?, ?> mappings) {
        return getFieldsWithSetting("type", type, mappings);
    }

    private static Set<String> getFieldsWithSetting(String setting, Object value, Map<?, ?> mappings) {
        return getFieldsWithSetting(setting, value::equals, mappings);
    }

    private static Set<String> getFieldsWithSetting(String setting, Predicate<Object> cmp, Map<?, ?> mappings) {
        return getFieldsByCondition(mappings, (f, v) -> v instanceof Map<?, ?> m && cmp.test(m.get(setting)));
    }

    private static Set<String> getFieldsWithSuffix(Map<?, ?> mappings, String suffix) {
        return getFieldsByCondition(mappings, (f, v) -> f.endsWith(suffix));
    }

    private static Set<String> getFieldsByCondition(Map<?, ?> mappings, BiPredicate<String, Object> p) {
        Set<String> fields = new HashSet<>();
        DocumentUtil.Visitor visitor = (v, path) -> {
            String field = path.stream()
                    .filter(s -> !"properties".equals(s))
                    .map(Object::toString)
                    .collect(Collectors.joining("."));
            if (p.test(field, v)) {
                fields.add(field);
            }
            return NOP;
        };
        DocumentUtil.traverse(mappings.get("properties"), visitor);
        return fields;
    }

    private static <T, U> Set<U> union (List<T> mappings, Function<T, Set<U>> f) {
        return mappings.stream()
                .map(f)
                .reduce(new HashSet<>(), (a,b) -> {a.addAll(b); return a;});
    }

}
