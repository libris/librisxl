package whelk.search2;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryParams {
    private final static int DEFAULT_LIMIT = 20;
    private final static int MAX_LIMIT = 4000;
    private final static int DEFAULT_OFFSET = 0;

    public static class ApiParams {
        public static final String QUERY = "_q";
        public static final String SORT = "_sort";
        public static final String LIMIT = "_limit";
        public static final String OFFSET = "_offset";
        public static final String LENS = "_lens";
        public static final String SPELL = "_spell";
        public static final String OBJECT = "_o";
        public static final String PREDICATES = "_p";
        public static final String DEBUG = "_debug";
        public static final String APP_CONFIG = "_appConfig";
        public static final String STATS = "_stats";
        public static final String ALIAS = "_alias-";
        public static final String SUGGEST = "_suggest";
        public static final String CURSOR = "cursor";
    }

    public static class Debug {
        public static final String ES_QUERY = "esQuery";
        public static final String ES_SCORE = "esScore";
    }

    public final int limit;
    public final int offset;
    public final Sort sortBy;
    public final String object;
    public final List<String> predicates;
    public final List<String> debug;
    public final String lens;
    public final Spell spell;
    public final String computedLabelLocale;
    public final Map<String, String[]> aliased;
    public final int cursor;

    public final String q;

    public final boolean skipStats;
    public final boolean suggest;

    public QueryParams(Map<String, String[]> apiParameters) throws InvalidQueryException {
        this.sortBy = Sort.fromString(getOptionalSingleNonEmpty(ApiParams.SORT, apiParameters).orElse(""));
        this.object = getOptionalSingleNonEmpty(ApiParams.OBJECT, apiParameters).orElse(null);
        this.predicates = getMultiple(ApiParams.PREDICATES, apiParameters);
        this.debug = getMultiple(ApiParams.DEBUG, apiParameters);
        this.limit = getLimit(apiParameters);
        this.offset = getOffset(apiParameters);
        this.lens = getOptionalSingleNonEmpty(ApiParams.LENS, apiParameters).orElse("cards");
        this.spell = new Spell(getOptionalSingleNonEmpty(ApiParams.SPELL, apiParameters).orElse(""));
        this.computedLabelLocale = getOptionalSingleNonEmpty(JsonLd.Platform.COMPUTED_LABEL, apiParameters).orElse(null);
        this.q = getOptionalSingle(ApiParams.QUERY, apiParameters).orElse("");
        this.suggest = getOptionalSingle(ApiParams.SUGGEST, apiParameters).map("true"::equalsIgnoreCase).isPresent();
        this.cursor = getCursor(apiParameters);
        this.skipStats = suggest || getOptionalSingle(ApiParams.STATS, apiParameters).map("false"::equalsIgnoreCase).isPresent();
        this.aliased = getAliased(apiParameters);
    }

    public Map<String, String> getFullParamsMap() {
        return getParamsMap(List.of(ApiParams.QUERY,
                ApiParams.SORT,
                ApiParams.OFFSET,
                ApiParams.LIMIT,
                ApiParams.LENS,
                ApiParams.SPELL,
                ApiParams.OBJECT,
                ApiParams.DEBUG,
                ApiParams.STATS,
                JsonLd.Platform.COMPUTED_LABEL));
    }

    public Map<String, String> getCustomParamsMap(List<String> apiParams) {
        return getParamsMap(apiParams);
    }

    private Map<String, String> getParamsMap(List<String> apiParams) {
        Map<String, String> params = new LinkedHashMap<>();

        for (String param : apiParams) {
            switch (param) {
                case ApiParams.QUERY -> {
                    if (!q.isEmpty()) {
                        params.put(ApiParams.QUERY, q);
                    }
                }
                case ApiParams.SORT -> {
                    var sort = sortBy.asString();
                    if (!sort.isEmpty()) {
                        params.put(ApiParams.SORT, sort);
                    }
                }
                case ApiParams.OFFSET -> {
                    if (offset > 0) {
                        params.put(ApiParams.OFFSET, "" + offset);
                    }
                }
                case ApiParams.LIMIT -> params.put(ApiParams.LIMIT, "" + limit);
                case ApiParams.SPELL -> {
                    var spellP = spell.asString();
                    if (!spellP.isEmpty()) {
                        params.put(ApiParams.SPELL, spellP);
                    }
                }
                case ApiParams.OBJECT -> {
                    if (object != null) {
                        params.put(ApiParams.OBJECT, object);
                    }
                }
                case ApiParams.PREDICATES -> {
                    if (!predicates.isEmpty()) {
                        params.put(ApiParams.PREDICATES, String.join(",", predicates));
                    }
                }
                case ApiParams.DEBUG -> {
                    if (!debug.isEmpty()) {
                        params.put(ApiParams.DEBUG, String.join(",", debug));
                    }
                }
                case ApiParams.STATS -> {
                    if (skipStats && !suggest) {
                        params.put(ApiParams.STATS, "false");
                    }
                }
            }
            if (param.equals(JsonLd.Platform.COMPUTED_LABEL) && computedLabelLocale != null) {
                params.put(JsonLd.Platform.COMPUTED_LABEL, computedLabelLocale);
            }
        }

        return params;
    }

    private static Optional<String> getOptionalSingleNonEmpty(String name, Map<String, String[]> queryParameters) {
        return getOptionalSingle(name, queryParameters).filter(Predicate.not(String::isEmpty));
    }

    private static Optional<String> getOptionalSingle(String name, Map<String, String[]> queryParameters) {
        return Optional.ofNullable(queryParameters.get(name))
                .map(x -> x[0]);
    }

    private static List<String> getMultiple(String name, Map<String, String[]> queryParameters) {
        return Optional.ofNullable(queryParameters.get(name))
                .map(Arrays::asList).orElse(Collections.emptyList())
                .stream()
                .flatMap((s -> Arrays.stream(s.split(",")).map(String::trim)))
                .toList();
    }

    private static Map<String, String[]> getAliased(Map<String, String[]> queryParameters) {
        return queryParameters.entrySet().stream()
                .filter((entry) -> entry.getKey().startsWith(ApiParams.ALIAS))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private int getLimit(Map<String, String[]> queryParameters) throws InvalidQueryException {
        int limit = getOptionalSingleNonEmpty(ApiParams.LIMIT, queryParameters)
                .map(x -> parseInt(x, DEFAULT_LIMIT))
                .orElse(DEFAULT_LIMIT);

        //TODO: Copied from old SearchUtils
        if (limit > MAX_LIMIT) {
            limit = DEFAULT_LIMIT;
        }

        if (limit < 0) {
            throw new InvalidQueryException(ApiParams.LIMIT + " query parameter can't be negative.");
        }

        return limit;
    }

    private int getOffset(Map<String, String[]> queryParameters) throws InvalidQueryException {
        int offset = getOptionalSingleNonEmpty(ApiParams.OFFSET, queryParameters)
                .map(x -> parseInt(x, DEFAULT_OFFSET))
                .orElse(DEFAULT_OFFSET);

        //TODO: Copied from old SearchUtils
        if (offset < 0) {
            throw new InvalidQueryException(ApiParams.OFFSET + " query parameter can't be negative.");
        }

        return offset;
    }

    private int getCursor(Map<String, String[]> queryParameters) {
        return getOptionalSingleNonEmpty(ApiParams.CURSOR, queryParameters)
                .map(x -> parseInt(x, -1))
                .orElse(-1);
    }

    private static int parseInt(String s, Integer defaultTo) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
            return defaultTo;
        }
    }
}
