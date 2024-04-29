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
    private static final List<SimpleQueryTree.PropertyValue> DEFAULT_FILTERS = List.of(SimpleQueryTree.pvEqualsVocabTerm("rdf:type", "Work"));

    Whelk whelk;
    XLQLQuery xlqlQuery;

    private static class P {
        public static final String QUERY = "_q";
        public static final String SIMPLE_FREETEXT = "_i";
        public static final String SORT = "_sort";
        public static final String LIMIT = "_limit";
        public static final String OFFSET = "offSet";
        public static final String OBJECT = "_o";
        public static final String PREDICATES = "_p";
        public static final String EXTRA = "_x";
        public static final String DEBUG = "_debug";
        public static final String STATS_REPRESENTATION = "_statsrepr";
    }

    private static class Debug {
        public static final String ES_QUERY = "esQuery";
    }

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
        private final List<String> predicates;
        private final String mode;
        private final Map<String, Object> statsRepr;
        private final List<String> debug;
        private final String queryString;
        private final String freeText;
        private final SimpleQueryTree simpleQueryTree;
        private final Disambiguate.OutsetType outsetType;
        private final QueryTree queryTree;
        private final Map<String, Object> esQueryDsl;

        Query(Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
            this.sortBy = Sort.fromString(getOptionalSingleNonEmpty(P.SORT, queryParameters).orElse(""));
            this.object = getOptionalSingleNonEmpty(P.OBJECT, queryParameters).orElse(null);
            this.predicates = getMultiple(P.PREDICATES, queryParameters);
            this.mode = getOptionalSingleNonEmpty(P.EXTRA, queryParameters).orElse(null);
            this.debug = getMultiple(P.DEBUG, queryParameters);
            this.limit = getLimit(queryParameters);
            this.offset = getOffset(queryParameters);
            this.statsRepr = getStatsRepr(queryParameters);

            var q = getOptionalSingle(P.QUERY, queryParameters);
            var i = getOptionalSingle(P.SIMPLE_FREETEXT, queryParameters);

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
            if (sortBy == Sort.DEFAULT_BY_RELEVANCY && queryTree.isWild()) {
                // Stable sort order if there is no meaningful relevancy
                Sort.BY_DOC_ID.insertSortClauses(queryDsl, xlqlQuery);
            } else {
                sortBy.insertSortClauses(queryDsl, xlqlQuery);
            }
            queryDsl.put("aggs", xlqlQuery.getAggQuery(statsRepr, outsetType));
            queryDsl.put("track_total_hits", true);
            return queryDsl;
        }

        public Map<String, Object> getPartialCollectionView(Map<String, Object> esResponse) {
            int numHits = (int) esResponse.getOrDefault("totalHits", 0);
            var aliases = XLQLQuery.getAliasMappings(outsetType);
            var view = new LinkedHashMap<String, Object>();
            view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
            view.put(JsonLd.ID_KEY, makeFindUrl(freeText, queryString, offset));
            view.put("itemOffset", offset);
            view.put("itemsPerPage", limit);
            view.put("totalItems", numHits);
            view.put("search", Map.of("mapping", toMappings(aliases)));
            view.putAll(makePaginationLinks(numHits));
            if (esResponse.containsKey("items")) {
                view.put("items", esResponse.get("items"));
            }
            view.put("stats", xlqlQuery.getStats(esResponse, statsRepr, simpleQueryTree, getNonQueryParams(0), aliases));
            if (debug.contains(Debug.ES_QUERY)) {
                view.put(P.DEBUG, Map.of(Debug.ES_QUERY, esQueryDsl));
            }
            view.put("maxItems", whelk.elastic.maxResultWindow);

            return view;
        }

        private SimpleQueryTree getFilteredTree() {
            var filters = new ArrayList<>(DEFAULT_FILTERS);
            if (object != null) {
                if (predicates.isEmpty()) {
                    filters.add(SimpleQueryTree.pvEqualsLink("_links", object));
                } else {
                    filters.addAll(predicates.stream().map(p -> SimpleQueryTree.pvEqualsLink(p, object)).toList());
                }
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
            params.add(XLQLQuery.makeParam(P.SIMPLE_FREETEXT, i));
            params.add(XLQLQuery.makeParam(P.QUERY, q));
            params.addAll(makeNonQueryParams(offset));
            return "/find?" + String.join("&", params);
        }

        private List<String> makeNonQueryParams(int offset) {
            return XLQLQuery.makeParams(getNonQueryParams(offset));
        }

        private Map<String, String> getNonQueryParams(int offset) {
            Map<String, String> params = new LinkedHashMap<>();
            if (offset > 0) {
                params.put(P.OFFSET, "" + offset);
            }
            params.put(P.LIMIT, "" + limit);
            if (object != null) {
                params.put(P.OBJECT, object);
            }
            if (!predicates.isEmpty()) {
                params.put(P.PREDICATES, String.join(",", predicates));
            }
            if (mode != null) {
                params.put(P.EXTRA, mode);
            }
            var sort = sortBy.asString();
            if (!sort.isEmpty()) {
                params.put(P.SORT, sort);
            }
            if (!debug.isEmpty()) {
                params.put(P.DEBUG, String.join(",", debug));
            }
            return params;
        }

        private List<Map<?, ?>> toMappings(Map<String, String> aliases) {
            return List.of(xlqlQuery.toMappings(simpleQueryTree, aliases, makeNonQueryParams(0)));
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
            int limit = getOptionalSingleNonEmpty(P.LIMIT, queryParameters)
                    .map(x -> parseInt(x, DEFAULT_LIMIT))
                    .orElse(DEFAULT_LIMIT);

            //TODO: Copied from old SearchUtils
            if (limit > MAX_LIMIT) {
                limit = DEFAULT_LIMIT;
            }

            if (limit < 0) {
                throw new InvalidQueryException(P.LIMIT + " query parameter can't be negative.");
            }

            return limit;
        }

        private int getOffset(Map<String, String[]> queryParameters) throws InvalidQueryException {
            int offset = getOptionalSingleNonEmpty(P.OFFSET, queryParameters)
                    .map(x -> parseInt(x, DEFAULT_OFFSET))
                    .orElse(DEFAULT_OFFSET);

            //TODO: Copied from old SearchUtils
            if (offset < 0) {
                throw new InvalidQueryException(P.OFFSET + " query parameter can't be negative.");
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

            var statsJson = Optional.ofNullable(queryParameters.get(P.STATS_REPRESENTATION))
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
        public static Sort DEFAULT_BY_RELEVANCY = new Sort("");
        public static Sort BY_DOC_ID = new Sort("_es_id");

        private enum Order {
            asc,
            desc
        }

        private record ParameterOrder(String parameter, Order order) {
            public Map<?, ?> toSortClause(XLQLQuery xlqlQuery) {
                // TODO nested?
                return Map.of(
                        xlqlQuery.getSortField(parameter), Map.of("order", order)
                );
            }

            public String asString() {
                return order == Order.desc
                        ? "-" + parameter
                        : parameter;
            }
        }

        List<ParameterOrder> parameters;

        private Sort(String sort) {
            parameters = Arrays.stream(sort.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(s -> s.startsWith("-")
                            ? new ParameterOrder(s.substring(1), Order.desc)
                            : new ParameterOrder(s, Order.asc))
                    .toList();
        }

        static Sort fromString(String s) {
            return (s == null || s.isBlank())
                    ? DEFAULT_BY_RELEVANCY
                    : new Sort(s);
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
