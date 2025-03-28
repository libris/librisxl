package whelk.search2;

import whelk.exception.InvalidQueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryParams {
    private final static int DEFAULT_LIMIT = 200;
    private final static int MAX_LIMIT = 4000;
    private final static int DEFAULT_OFFSET = 0;

    public static class ApiParams {
        public static final String QUERY = "_q";
        public static final String SIMPLE_FREETEXT = "_i";
        public static final String SORT = "_sort";
        public static final String LIMIT = "_limit";
        public static final String OFFSET = "_offset";
        public static final String LENS = "_lens";
        public static final String SPELL = "_spell";
        public static final String OBJECT = "_o";
        public static final String PREDICATES = "_p";
        public static final String EXTRA = "_x";
        public static final String DEBUG = "_debug";
        public static final String APP_CONFIG = "_appConfig";
        public static final String BOOST = "_boost";
        public static final String STATS = "_stats";
        public static final String FN_SCORE = "_fnScore";
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
    public final String mode;
    public final List<String> debug;
    public final String lens;
    public final Spell spell;
    public final List<String> boostFields;
    public final List<EsBoost.ScoreFunction> esScoreFunctions;

    public final String q;
    public final String i;

    public final boolean skipStats;

    public QueryParams(Map<String, String[]> apiParameters) throws InvalidQueryException {
        this.sortBy = Sort.fromString(getOptionalSingleNonEmpty(ApiParams.SORT, apiParameters).orElse(""));
        this.object = getOptionalSingleNonEmpty(ApiParams.OBJECT, apiParameters).orElse(null);
        this.predicates = getMultiple(ApiParams.PREDICATES, apiParameters);
        this.mode = getOptionalSingleNonEmpty(ApiParams.EXTRA, apiParameters).orElse(null);
        this.debug = getMultiple(ApiParams.DEBUG, apiParameters);
        this.limit = getLimit(apiParameters);
        this.offset = getOffset(apiParameters);
        this.lens = getOptionalSingleNonEmpty(ApiParams.LENS, apiParameters).orElse("cards");
        this.spell = new Spell(getOptionalSingleNonEmpty(ApiParams.SPELL, apiParameters).orElse(""));
        this.boostFields = getMultiple(ApiParams.BOOST, apiParameters);
        this.q = getOptionalSingle(ApiParams.QUERY, apiParameters).orElse("");
        this.i = getOptionalSingle(ApiParams.SIMPLE_FREETEXT, apiParameters).orElse("");
        this.skipStats = getOptionalSingle(ApiParams.STATS, apiParameters).map("false"::equalsIgnoreCase).isPresent();
        this.esScoreFunctions = getEsScoreFunctions(apiParameters);
    }

    public Map<String, String> getNonQueryParams() {
        return getNonQueryParams(offset);
    }

    public Map<String, String> getNonQueryParams(int offset) {
        Map<String, String> params = new LinkedHashMap<>();
        if (offset > 0) {
            params.put(ApiParams.OFFSET, "" + offset);
        }
        params.put(ApiParams.LIMIT, "" + limit);
        if (object != null) {
            params.put(ApiParams.OBJECT, object);
        }
        if (!predicates.isEmpty()) {
            params.put(ApiParams.PREDICATES, String.join(",", predicates));
        }
        if (mode != null) {
            params.put(ApiParams.EXTRA, mode);
        }
        var spellP = spell.asString();
        if (!spellP.isEmpty()) {
            params.put(ApiParams.SPELL, spellP);
        }
        var sort = sortBy.asString();
        if (!sort.isEmpty()) {
            params.put(ApiParams.SORT, sort);
        }
        if (!debug.isEmpty()) {
            params.put(ApiParams.DEBUG, String.join(",", debug));
        }
        if (!boostFields.isEmpty()) {
            params.put(ApiParams.BOOST, String.join(",", boostFields));
        }
        if (skipStats) {
            params.put(ApiParams.STATS, "false");
        }
        if (!esScoreFunctions.isEmpty()) {
            params.put(ApiParams.FN_SCORE,
                    esScoreFunctions.stream()
                            .map(f -> String.join(";", f.paramList()))
                            .collect(Collectors.joining(","))
            );
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

    private static int parseInt(String s, int defaultTo) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
            return defaultTo;
        }
    }

    private List<EsBoost.ScoreFunction> getEsScoreFunctions(Map<String, String[]> queryParameters) {
        List<EsBoost.ScoreFunction> scoreFunctions = new ArrayList<>();
        try {
            getMultiple(ApiParams.FN_SCORE, queryParameters).stream()
                    .map(s -> s.split(";"))
                    .forEach(fieldConfig -> {
                        String fnType = fieldConfig[0];
                        if (fnType.equalsIgnoreCase("fvf")) {
                            String field = fieldConfig[1];
                            float factor = Float.parseFloat(fieldConfig[2]);
                            String modifier = fieldConfig[3];
                            float missing = Float.parseFloat(fieldConfig[4]);
                            float weight = Float.parseFloat(fieldConfig[5]);
                            scoreFunctions.add(new EsBoost.FieldValueFactor(field, factor, modifier, missing, weight));
                        }
                        if (fnType.equalsIgnoreCase("mfv")) {
                            String field = fieldConfig[1];
                            String value = fieldConfig[2];
                            float boost = Float.parseFloat(fieldConfig[3]);
                            scoreFunctions.add(new EsBoost.MatchingFieldValue(field, value, boost));
                        }
                    });
        } catch (Exception ignored) {
        }
        return scoreFunctions;
    }
}
