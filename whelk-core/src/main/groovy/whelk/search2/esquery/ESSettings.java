package whelk.search2.esquery;

import whelk.Whelk;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static whelk.JsonLd.SEARCH_KEY;
import static whelk.component.ElasticSearch.SystemFields.FLATTENED_LANG_MAP_PREFIX;
import static whelk.component.ElasticSearch.SystemFields.CARD_STR;
import static whelk.component.ElasticSearch.SystemFields.CHIP_STR;
import static whelk.component.ElasticSearch.SystemFields.ES_ID;
import static whelk.component.ElasticSearch.SystemFields.IDS;
import static whelk.component.ElasticSearch.SystemFields.LINKS;
import static whelk.component.ElasticSearch.SystemFields.OUTER_EMBELLISHMENTS;
import static whelk.component.ElasticSearch.SystemFields.SEARCH_CARD_STR;
import static whelk.component.ElasticSearch.SystemFields.SORT_KEY_BY_LANG;
import static whelk.component.ElasticSearch.SystemFields.TOP_STR;
import static whelk.util.Jackson.mapper;

public class ESSettings {
    private static final String BOOST_SETTINGS_FILE = "libris_search_boost.json";

    private ESMappings mappings;
    private final ESBoost boost;
    private final List<String> sourceExcludes;

    private int maxItems;

    public ESSettings(Whelk whelk) {
        if (whelk.elastic != null) {
            this.mappings = new ESMappings(whelk.elastic.getAllMappings());
            this.maxItems = whelk.elastic.maxResultWindow;
        }
        this.boost = loadBoostSettings();
        this.sourceExcludes = loadSourceExcludesSettings();
    }

    // For test only
    public ESSettings(ESMappings mappings, ESBoost boost) {
       this(mappings, boost, 1);
    }

    public ESSettings withBoostSettings(ESBoost boost) {
        return new ESSettings(mappings, boost, maxItems);
    }

    private ESSettings(ESMappings mappings, ESBoost boost, int maxItems) {
        this.mappings = mappings;
        this.boost = boost;
        this.maxItems = maxItems;
        this.sourceExcludes = Collections.emptyList();
    }

    public boolean isConfigured() {
        return mappings != null;
    }

    public ESMappings mappings() {
        return mappings;
    }

    public ESBoost boost() {
        return boost;
    }

    public List<String> sourceExcludes() {
        return sourceExcludes;
    }

    public int maxItems() {
        return maxItems;
    }

    public ESBoost loadBoostSettings() {
        Map<?, ?> settings = toMap(ESBoost.class.getClassLoader().getResourceAsStream(BOOST_SETTINGS_FILE));
        return new ESBoost(settings);
    }

    private List<String> loadSourceExcludesSettings() {
        var systemSourceExcludes = List.of(
                ES_ID,
                LINKS,
                OUTER_EMBELLISHMENTS,
                SORT_KEY_BY_LANG,

                IDS,
                TOP_STR,
                CHIP_STR,
                CARD_STR,
                SEARCH_CARD_STR,

                "*." + FLATTENED_LANG_MAP_PREFIX + "*",
                "*." + SEARCH_KEY
        );

        Map<?, ?> settings = toMap(ESBoost.class.getClassLoader().getResourceAsStream(BOOST_SETTINGS_FILE));
        return Stream.concat(
                systemSourceExcludes.stream(),
                getAsStream(settings, "source_excludes").map(String.class::cast)
        ).toList();
    }

    public static ESBoost loadBoostSettings(String json) {
        return new ESBoost(toMap(json));
    }
//
    private static Stream<?> getAsStream(Map<?, ?> m, String k) {
        return getOrDefault(m, k, List.of()).stream();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getOrDefault(Map<?, ?> m, String k, T defaultTo) {
        return m.containsKey(k) ? (T) m.get(k) : defaultTo;
    }

    private static Map<?, ?> toMap(Object json) {
        try {
            if (json instanceof String) {
                return mapper.readValue((String) json, Map.class);
            } else if (json instanceof InputStream) {
                return mapper.readValue((InputStream) json, Map.class);
            }
        } catch (IOException ignored) {
            return Map.of();
        }
        return Map.of();
    }
}
