package whelk.rest.api;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.exception.WhelkRuntimeException;
import whelk.search.XLQLQuery;
import whelk.xlql.Disambiguate;
import whelk.xlql.QueryTree;
import whelk.xlql.SimpleQueryTree;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static whelk.util.Jackson.mapper;

public class SearchUtils2 {
    final static int DEFAULT_LIMIT = 200;
    final static int MAX_LIMIT = 4000;
    final static int DEFAULT_OFFSET = 0;

    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper();

    Whelk whelk;
    XLQLQuery xlqlQuery;

    SearchUtils2(Whelk whelk) {
        this.whelk = whelk;
        this.xlqlQuery = new XLQLQuery(whelk);
    }

    Map<String, Object> doSearch(Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
        if (whelk.elastic == null) {
            throw new WhelkRuntimeException("ElasticSearch not configured.");
        }
        Query query = new Query(queryParameters);
        @SuppressWarnings("unchecked")
        var esResponse = (Map<String, Object>) whelk.elastic.query(query.getEsQueryDsl());
        return query.getPartialCollectionView(esResponse);
    }

    class Query {
        //        static Set<String> reservedParameters = getReservedParameters();
        private final int limit;
        private final int offset;
        private final Optional<String> sortBy;
        private final Map<?, ?> statsRepr;
        private final boolean debug;
        private final String queryString;
        private final SimpleQueryTree simpleQueryTree;
        private final Disambiguate.OutsetType outsetType;
        private final QueryTree queryTree;
        private final Map<String, Object> esQueryDsl;
//    Optional<List> predicates;
//    Optional<String> object;
//    Optional<String> value;
//    Optional<String> lens;
//    Optional<String> addStats;
//    Optional<String> suggest;

        Query(Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
            this.queryString = queryParameters.get("_q")[0];
            this.sortBy = getOptionalSingle("_sort", queryParameters);
            this.debug = queryParameters.containsKey("_debug"); // Different debug modes needed?
            this.limit = getLimit(queryParameters);
            this.offset = getOffset(queryParameters);
            this.statsRepr = getStatsRepr(queryParameters);
            this.simpleQueryTree = xlqlQuery.getSimpleQueryTree(queryString);
            this.outsetType = xlqlQuery.getOutsetType(simpleQueryTree);
            this.queryTree = xlqlQuery.getQueryTree(simpleQueryTree, outsetType);
            this.esQueryDsl = getEsQueryDsl();
        }

        public Map<String, Object> getEsQueryDsl() {
            var queryDsl = new LinkedHashMap<String, Object>();
            queryDsl.put("query", xlqlQuery.getEsQuery(queryTree));
            queryDsl.put("size", limit);
            queryDsl.put("from", offset);
            sortBy.ifPresent(s -> queryDsl.put("sort", getSortClauses(s)));
            queryDsl.put("aggs", xlqlQuery.getAggQuery(statsRepr, outsetType));
            queryDsl.put("track_total_hits", true);
            return queryDsl;
        }

        public Map<String, Object> getPartialCollectionView(Map<String, Object> esResponse) {
            int numHits = (int) esResponse.getOrDefault("totalHits", 0);
            var view = new LinkedHashMap<String, Object>();
            view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
            view.put(JsonLd.ID_KEY, makeFindUrl(offset));
            view.put("itemOffset", offset);
            view.put("itemsPerPage", limit);
            view.put("totalItems", numHits);
            view.put("search", Map.of("mapping", toMappings()));
            view.putAll(makePaginationLinks(numHits));
            if (esResponse.containsKey("items")) {
                view.put("items", esResponse.get("items"));
            }
            // TODO: Stats?
            if (debug) {
                view.put("_debug", Map.of("esQuery", esQueryDsl));
            }
            view.put("maxItems", whelk.elastic.maxResultWindow);

            return view;
        }

        private Map<String, Map<String, String>> makePaginationLinks(int numHits) {
            if (limit == 0) {
                // we don't have anything to paginate over
                return Collections.emptyMap();
            }

            var result = new LinkedHashMap<String, Map<String, String>>();

            Offsets offsets = new Offsets(Math.min(numHits, maxItems()), limit, offset);

            result.put("first", Map.of(JsonLd.ID_KEY, makeFindUrl(0)));
            result.put("last", Map.of(JsonLd.ID_KEY, makeFindUrl(offsets.last)));

            if (offsets.prev != null) {
                if (offsets.prev == 0) {
                    result.put("previous", result.get("first"));
                } else {
                    result.put("previous", Map.of(JsonLd.ID_KEY, makeFindUrl(offsets.prev)));
                }
            }

            if (offsets.next != null) {
                result.put("next", Map.of(JsonLd.ID_KEY, makeFindUrl(offsets.next)));
            }

            return result;
        }

        private int maxItems() {
            return whelk.elastic.maxResultWindow;
        }

        private String makeFindUrl(int offset) {
            List<String> params = new ArrayList<>();
            params.add(makeParam("_q", queryString));
            params.addAll(makeNonQueryParams(offset));
            return "/find?" + String.join("&", params);
        }

        private List<String> makeNonQueryParams(int offset) {
            List<String> params = new ArrayList<>();
            if (offset > 0) {
                params.add(makeParam("_offset", offset));
            }
            params.add(makeParam("_limit", limit));
            return params;
        }

        private static String makeParam(String key, String value) {
            return String.format("%s=%s", escapeQueryParam(key), escapeQueryParam(value));
        }
        private static String makeParam(String key, int value) {
            return makeParam(key, "" + value);
        }

        private List<Map<?, ?>> toMappings() {
            return List.of(xlqlQuery.toMappings(simpleQueryTree, makeNonQueryParams(0)));
        }

        private static String escapeQueryParam(String input) {
            return QUERY_ESCAPER.escape(input)
                    // We want pretty URIs, restore some characters which are inside query strings
                    // https://tools.ietf.org/html/rfc3986#section-3.4
                    .replace("%3A", ":")
                    .replace("%2F", "/")
                    .replace("%40", "@");
        }

        List<Map<String, Object>> getSortClauses(String sortBy) {
            // TODO
            return List.of(Map.of(
                    "meta.modified", Map.of("order", "asc")
            ));
        }

        private Optional<String> getOptionalSingle(String name, Map<String, String[]> queryParameters) {
            return Optional.ofNullable(queryParameters.get(name))
                    .map(x -> x[0])
                    .filter(x -> !x.isEmpty());
        }

        private int getLimit(Map<String, String[]> queryParameters) throws InvalidQueryException {
            int limit = getOptionalSingle("_limit", queryParameters)
                    .map(x -> parseInt(x, DEFAULT_LIMIT))
                    .orElse(DEFAULT_LIMIT);

            //TODO: Copied from old SearchUtils
            if (limit > MAX_LIMIT) {
                limit = DEFAULT_LIMIT;
            }

            if (limit < 0) {
                throw new InvalidQueryException("\"_limit\" query parameter can't be negative.");
            }

            return limit;
        }

        private int getOffset(Map<String, String[]> queryParameters) throws InvalidQueryException {
            int offset = getOptionalSingle("_offset", queryParameters)
                    .map(x -> parseInt(x, DEFAULT_OFFSET))
                    .orElse(DEFAULT_OFFSET);

            //TODO: Copied from old SearchUtils
            if (offset < 0) {
                throw new InvalidQueryException("\"_offset\" query parameter can't be negative.");
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

        private static Map<?,?> getStatsRepr(Map<String, String[]> queryParameters) throws IOException {
            var statsJson = Optional.ofNullable(queryParameters.get("_statsrepr"))
                    .map(x -> x[0])
                    .orElse("{}");

            return mapper.readValue(statsJson, Map.class);
        }
    }

    static class Offsets {
        Integer prev;
        Integer next;
        Integer last;

        Offsets(int total, int limit, int offset) throws IllegalArgumentException {
            if (limit <= 0) {
                throw new IllegalArgumentException("\"limit\" must be greater than 0.");
            }

            if (offset < 0) {
                throw new IllegalArgumentException("\"offset\" can't be negative.");
            }

            this.prev = offset - limit;
            if (this.prev < 0) {
                this.prev = null;
            }

            this.next = offset + limit;
            if (this.next >= total) {
                this.next = null;
            } else if (offset == 0) {
                this.next = limit;
            }

            if ((offset + limit) >= total) {
                this.last = offset;
            } else {
                if (total % limit == 0) {
                    this.last = total - limit;
                } else {
                    this.last = total - (total % limit);
                }
            }
        }
    }
}
