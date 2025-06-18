package whelk.search2;

import whelk.util.DocumentUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static whelk.util.DocumentUtil.NOP;

public class EsMappings {
    private final Set<String> keywordFields;
    private final Set<String> dateFields;
    private final Set<String> nestedFields;
    private final Set<String> longFields;
    private final Set<String> nestedNotInParentFields;
    private final Set<String> numericExtractorFields;

    private final boolean isSpellCheckAvailable;

    public EsMappings(Map<?, ?> mappings) {
        this.keywordFields = getKeywordFields(mappings);
        this.dateFields = getFieldsOfType("date", mappings);
        this.nestedFields = getFieldsOfType("nested", mappings);
        this.longFields = getFieldsOfType("long", mappings);
        this.nestedNotInParentFields = new HashSet<>(nestedFields);
        this.nestedNotInParentFields.removeAll(getFieldsWithSetting("include_in_parent", true, mappings));
        this.numericExtractorFields = getFieldsWithAnalyzer("numeric_extractor", mappings);

        // TODO: temporary feature flag, to be removed
        // this feature only works after a full reindex has been done, so we have to detect that
        this.isSpellCheckAvailable = DocumentUtil.getAtPath(mappings, List.of("properties", "_sortKeyByLang", "properties", "sv", "fields", "trigram"), null) != null;
    }

    public boolean isKeywordField(String fieldPath) {
        return keywordFields.contains(fieldPath);
    }

    public boolean isDateField(String fieldPath) {
        return dateFields.contains(fieldPath);
    }

    public boolean isNestedField(String fieldPath) {
        return nestedFields.contains(fieldPath);
    }

    public boolean isLongField(String fieldPath) {
        return longFields.contains(fieldPath);
    }

    public boolean isNestedNotInParentField(String fieldPath) {
        return nestedNotInParentFields.contains(fieldPath);
    }

    public boolean isFourDigitField(String fieldPath) {
        return numericExtractorFields.contains(fieldPath);
    }

    public Set<String> getNestedFields() {
        return nestedFields;
    }

    public boolean isSpellCheckAvailable() {
        return isSpellCheckAvailable;
    }

    private static Set<String> getKeywordFields(Map<?, ?> mappings) {
        Predicate<Object> test = v -> v instanceof Map && ((Map<?, ?>) v).containsKey("keyword");
        return getFieldsWithSetting("fields", test, mappings);
    }

    private static Set<String> getFieldsOfType(String type, Map<?, ?> mappings) {
        return getFieldsWithSetting("type", type, mappings);
    }

    private static Set<String> getFieldsWithAnalyzer(String analyzer, Map<?, ?> mappings) {
        return getFieldsWithSetting("analyzer", analyzer, mappings);
    }

    private static Set<String> getFieldsWithSetting(String setting, Object value, Map<?, ?> mappings) {
        return getFieldsWithSetting(setting, value::equals, mappings);
    }

    private static Set<String> getFieldsWithSetting(String setting, Predicate<Object> cmp, Map<?, ?> mappings) {
        Set<String> fields = new HashSet<>();
        DocumentUtil.Visitor visitor = (v, path) -> {
            if (cmp.test(v)) {
                var p = dropLast(path).stream()
                        .filter(s -> !"properties".equals(s))
                        .map(Object::toString)
                        .toList();
                var field = String.join(".", p);
                fields.add(field);
            }
            return NOP;
        };
        DocumentUtil.findKey(mappings.get("properties"), setting, visitor);
        return fields;
    }

    static <V> List<V> dropLast(List<V> list) {
        var l = new ArrayList<>(list);
        l.removeLast();
        return l;
    }
}
