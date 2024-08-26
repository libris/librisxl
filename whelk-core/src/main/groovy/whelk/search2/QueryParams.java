package whelk.search2;

import whelk.exception.InvalidQueryException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static whelk.util.Jackson.mapper;

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
    }

    public static class Debug {
        public static final String ES_QUERY = "esQuery";
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

    public final String q;
    public final String i;

    public QueryParams(Map<String, String[]> apiParameters) throws InvalidQueryException,
            IOException {
        this.sortBy = Sort.fromString(getOptionalSingleNonEmpty(ApiParams.SORT, apiParameters).orElse(""));
        this.object = getOptionalSingleNonEmpty(ApiParams.OBJECT, apiParameters).orElse(null);
        this.predicates = getMultiple(ApiParams.PREDICATES, apiParameters);
        this.mode = getOptionalSingleNonEmpty(ApiParams.EXTRA, apiParameters).orElse(null);
        this.debug = getMultiple(ApiParams.DEBUG, apiParameters);
        this.limit = getLimit(apiParameters);
        this.offset = getOffset(apiParameters);
        this.lens = getOptionalSingleNonEmpty(ApiParams.LENS, apiParameters).orElse("cards");
        this.spell = new Spell(getOptionalSingleNonEmpty(ApiParams.SPELL, apiParameters).orElse(""));
        this.q = getOptionalSingle(ApiParams.QUERY, apiParameters).orElse("");
        this.i = getOptionalSingle(ApiParams.SIMPLE_FREETEXT, apiParameters).orElse("");
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
}
