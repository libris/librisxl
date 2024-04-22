package whelk.rest.api;

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
import java.util.function.Predicate;

import static whelk.util.Jackson.mapper;

public class SearchUtils2 {
    final static int DEFAULT_LIMIT = 200;
    final static int MAX_LIMIT = 4000;
    final static int DEFAULT_OFFSET = 0;
    private static final List<SimpleQueryTree.PropertyValue> DEFAULT_FILTERS = List.of(SimpleQueryTree.pvEquals("rdf:type", "Work"));

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

    public Map<String, Object> buildStatsReprFromSliceSpec(List<Map<String, Object>> sliceList) {
        Map<String, Object> statsfind = new LinkedHashMap<>();
        for (Map<String, Object> slice : sliceList) {
            String key = (String) ((List<?>) slice.get("dimensionChain")).getFirst();
            int limit = (Integer) slice.get("itemLimit");
            var m = Map.of("sort", "value",
                    "sortOrder", "desc",
                    "size", limit);
            statsfind.put(key, m);
        }
        return statsfind;
    }

    class Query {
        private final int limit;
        private final int offset;
        private final Sort sortBy;
        private final String object;
        private final String mode;
        private final Map<String, Object> statsRepr;
        private final boolean debug;
        private final String queryString;
        private final String freeText;
        private final SimpleQueryTree simpleQueryTree;
        private final Disambiguate.OutsetType outsetType;
        private final QueryTree queryTree;
        private final Map<String, Object> esQueryDsl;

        Query(Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
            this.sortBy = getOptionalSingleFilterEmpty("_sort", queryParameters).map(Sort::parse).orElse(Sort.defaultSort());
            this.object = getOptionalSingleFilterEmpty("_o", queryParameters).orElse(null);
            this.mode = getOptionalSingleFilterEmpty("_x", queryParameters).orElse(null);
            this.debug = queryParameters.containsKey("_debug"); // Different debug modes needed?
            this.limit = getLimit(queryParameters);
            this.offset = getOffset(queryParameters);
            this.statsRepr = getStatsRepr(queryParameters);

            var q = getOptionalSingle("_q", queryParameters);
            var i = getOptionalSingle("_i", queryParameters);

            if (q.isPresent() && i.isPresent()) {
                var iSqt = xlqlQuery.getSimpleQueryTree(i.get());
                if (iSqt.isEmpty() || iSqt.isFreeText()) {
                    var qSqt = xlqlQuery.getSimpleQueryTree(q.get());
                    if (i.get().equals(qSqt.getFreeTextPart())) {
                        // The acceptable case
                        this.queryString = q.get();
                        this.freeText = i.get();
                        this.simpleQueryTree = qSqt;
                        SimpleQueryTree filteredTree = getFilteredTree();
                        this.outsetType = xlqlQuery.getOutsetType(filteredTree);
                        this.queryTree = xlqlQuery.getQueryTree(filteredTree, outsetType);
                        this.esQueryDsl = getEsQueryDsl();
                    } else {
                        qSqt.replaceTopLevelFreeText(i.get());
                        throw new Crud.RedirectException(makeFindUrl(i.get(), qSqt.toQueryString()));
                    }
                } else {
                    throw new Crud.RedirectException(makeFindUrl(iSqt.getFreeTextPart(), i.get()));
                }
            } else if (q.isPresent()) {
                var qSqt = xlqlQuery.getSimpleQueryTree(q.get());
                throw new Crud.RedirectException(makeFindUrl(qSqt.getFreeTextPart(), q.get()));
            } else if (i.isPresent()) {
                var iSqt = xlqlQuery.getSimpleQueryTree(i.get());
                throw new Crud.RedirectException(makeFindUrl(iSqt.getFreeTextPart(), i.get()));
            } else if (object != null) {
                throw new Crud.RedirectException(makeFindUrl("*", "*"));
            } else {
                throw new InvalidQueryException("Missing required query parameters");
            }
        }

        public Map<String, Object> getEsQueryDsl() {
            var queryDsl = new LinkedHashMap<String, Object>();
            queryDsl.put("query", xlqlQuery.getEsQuery(queryTree));
            queryDsl.put("size", limit);
            queryDsl.put("from", offset);
            sortBy.insertSortClauses(queryDsl, xlqlQuery);
            queryDsl.put("aggs", xlqlQuery.getAggQuery(statsRepr, outsetType));
            queryDsl.put("track_total_hits", true);
            return queryDsl;
        }

        public Map<String, Object> getPartialCollectionView(Map<String, Object> esResponse) {
            int numHits = (int) esResponse.getOrDefault("totalHits", 0);
            var view = new LinkedHashMap<String, Object>();
            view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
            view.put(JsonLd.ID_KEY, makeFindUrl(freeText, queryString, offset));
            view.put("itemOffset", offset);
            view.put("itemsPerPage", limit);
            view.put("totalItems", numHits);
            view.put("search", Map.of("mapping", toMappings()));
            view.putAll(makePaginationLinks(numHits));
            if (esResponse.containsKey("items")) {
                view.put("items", esResponse.get("items"));
            }
            view.put("stats", xlqlQuery.getStats(esResponse, statsRepr, simpleQueryTree, makeNonQueryParams(0)));
            if (debug) {
                view.put("_debug", Map.of("esQuery", esQueryDsl));
            }
            view.put("maxItems", whelk.elastic.maxResultWindow);

            return view;
        }

        private SimpleQueryTree getFilteredTree() {
            var filters = new ArrayList<>(DEFAULT_FILTERS);
            if (object != null) {
                filters.add(SimpleQueryTree.pvEquals("_links", object));
            }
            return xlqlQuery.addFilters(simpleQueryTree, filters);
        }

        private Map<String, Map<String, String>> makePaginationLinks(int numHits) {
            if (limit == 0) {
                // we don't have anything to paginate over
                return Collections.emptyMap();
            }

            var result = new LinkedHashMap<String, Map<String, String>>();

            Offsets offsets = new Offsets(Math.min(numHits, maxItems()), limit, offset);

            result.put("first", Map.of(JsonLd.ID_KEY, makeFindUrl(freeText, queryString)));
            result.put("last", Map.of(JsonLd.ID_KEY, makeFindUrl(freeText, queryString, offsets.last)));

            if (offsets.prev != null) {
                if (offsets.prev == 0) {
                    result.put("previous", result.get("first"));
                } else {
                    result.put("previous", Map.of(JsonLd.ID_KEY, makeFindUrl(freeText, queryString, offsets.prev)));
                }
            }

            if (offsets.next != null) {
                result.put("next", Map.of(JsonLd.ID_KEY, makeFindUrl(freeText, queryString, offsets.next)));
            }

            return result;
        }

        private int maxItems() {
            return whelk.elastic.maxResultWindow;
        }

        private String makeFindUrl(String i, String q) {
            return makeFindUrl(i, q, 0);
        }

        private String makeFindUrl(String i, String q, int offset) {
            List<String> params = new ArrayList<>();
            params.add(XLQLQuery.makeParam("_i", i));
            params.add(XLQLQuery.makeParam("_q", q));
            params.addAll(makeNonQueryParams(offset));
            return "/find?" + String.join("&", params);
        }

        private List<String> makeNonQueryParams(int offset) {
            List<String> params = new ArrayList<>();
            if (offset > 0) {
                params.add(makeParam("_offset", offset));
            }
            params.add(makeParam("_limit", limit));
            if (object != null) {
                params.add(makeParam("_o", object));
            }
            if (mode != null) {
                params.add(makeParam("_x", mode));
            }
            var sort = sortBy.asString();
            if (!sort.isEmpty()) {
                params.add(makeParam("_sort", sort));
            }
            return params;
        }

        private static String makeParam(String key, int value) {
            return makeParam(key, "" + value);
        }

        private static String makeParam(String key, String value) {
            return XLQLQuery.makeParam(key, value);
        }

        private List<Map<?, ?>> toMappings() {
            return List.of(xlqlQuery.toMappings(simpleQueryTree, makeNonQueryParams(0)));
        }

        private static Optional<String> getOptionalSingleFilterEmpty(String name, Map<String, String[]> queryParameters) {
            return getOptionalSingle(name, queryParameters).filter(Predicate.not(String::isEmpty));
        }

        private static Optional<String> getOptionalSingle(String name, Map<String, String[]> queryParameters) {
            return Optional.ofNullable(queryParameters.get(name))
                    .map(x -> x[0]);
        }

        private int getLimit(Map<String, String[]> queryParameters) throws InvalidQueryException {
            int limit = getOptionalSingleFilterEmpty("_limit", queryParameters)
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
            int offset = getOptionalSingleFilterEmpty("_offset", queryParameters)
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

        private static Map<String, Object> getStatsRepr(Map<String, String[]> queryParameters) throws IOException {
            Map<String, Object> statsRepr = new LinkedHashMap<>();

            var statsJson = Optional.ofNullable(queryParameters.get("_statsrepr"))
                    .map(x -> x[0])
                    .orElse("{}");

            Map<?, ?> statsMap = mapper.readValue(statsJson, LinkedHashMap.class);
            for (var entry : statsMap.entrySet()) {
                statsRepr.put((String) entry.getKey(), entry.getValue());
            }

            return statsRepr;
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

    static class Sort {
        private enum Order {
            asc,
            desc
        }

        private record ParameterOrder(String parameter, Order order) {
            public Map<?, ?> toSortClause(XLQLQuery xlqlQuery) {
                // TODO nested
                return Map.of(
                        xlqlQuery.getInferredSortTermPath(parameter), Map.of("order", order)
                );
            }

            public String asString() {
                return order == Order.desc
                        ? "-" + parameter
                        : parameter;
            }

        }

        List<ParameterOrder> parameters;

        public Sort(String sort) {
            parameters = Arrays.stream(sort.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(s -> s.startsWith("-")
                            ? new ParameterOrder(s.substring(1), Order.desc)
                            : new ParameterOrder(s, Order.asc))
                    .toList();
        }

        static Sort defaultSort() {
            return new Sort("");
        }

        static Sort parse(String s) {
            return new Sort(s);
        }

        void insertSortClauses(Map<String, Object> queryDsl, XLQLQuery xlqlQuery) {
            if (!parameters.isEmpty()) {
                queryDsl.put("sort", parameters.stream().map(f -> f.toSortClause(xlqlQuery)).toList());
            }
        }

        String asString() {
            return String.join(",", parameters.stream().map(ParameterOrder::asString).toList());
        }
    }
}
