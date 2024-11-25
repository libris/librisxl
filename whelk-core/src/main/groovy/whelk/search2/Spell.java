package whelk.search2;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.querytree.QueryTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class Spell {
    public record Suggestion(String text, String highlighted) {
    }

    public final boolean suggest;
    public final boolean suggestOnly;

    Spell(String s) {
        this.suggest = s.equals("true") || s.equals("only");
        this.suggestOnly = s.equals("only");
    }

    public static Optional<Map<String, Object>> getSpellQuery(QueryTree qt) {
        return Optional.of(qt.getTopLevelFreeText())
                .filter(Predicate.not(String::isEmpty))
                .map(ESQuery::getSpellQuery)
                .map(QueryUtil::castToStringObjectMap);
    }

    public static List<Map<String, Object>> buildSpellSuggestions(QueryResult queryResult, QueryTree qt,
                                                      Map<String, String> nonQueryParams) {
        List<Map<String, Object>> suggestions = new ArrayList<>();
        for (Suggestion s : queryResult.spell) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("label", s.text());
            m.put("labelHtml", s.highlighted());
            m.put("view", Map.of(JsonLd.ID_KEY, QueryUtil.makeFindUrl(qt.replaceFreeText(s.text()), nonQueryParams)));
            suggestions.add(m);
        }
        return suggestions;
    }

    public static List<Suggestion> collectSuggestions(Map<String, Object> esResponse) {
        return ((List<?>) esResponse.getOrDefault("spell", Collections.emptyList()))
                .stream()
                .map(Map.class::cast)
                .map(m -> new Suggestion((String) m.get("text"), (String) m.get("highlighted")))
                .toList();
    }

    public String asString() {
        if (suggestOnly) {
            return "only";
        }
        if (suggest) {
            return "true";
        }
        return "";
    }
}
