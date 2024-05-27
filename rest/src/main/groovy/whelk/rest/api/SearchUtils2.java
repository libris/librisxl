package whelk.rest.api;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.exception.WhelkRuntimeException;
import whelk.search.XLQLQuery;
import whelk.util.DocumentUtil;
import whelk.xlql.Disambiguate;
import whelk.xlql.QueryTree;
import whelk.xlql.SimpleQueryTree;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static whelk.util.Jackson.mapper;
import static whelk.xlql.Disambiguate.RDF_TYPE;
import static whelk.xlql.SimpleQueryTree.pvEqualsLink;

public class SearchUtils2 {
    final static int DEFAULT_LIMIT = 200;
    final static int MAX_LIMIT = 4000;
    final static int DEFAULT_OFFSET = 0;

    Whelk whelk;
    XLQLQuery xlqlQuery;

    public static class P {
        public static final String QUERY = "_q";
        public static final String SIMPLE_FREETEXT = "_i";
        public static final String SORT = "_sort";
        public static final String LIMIT = "_limit";
        public static final String OFFSET = "_offset";
        public static final String LENS = "_lens";
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

        // TODO: Use Multi search API instead of doing two elastic roundtrips?
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html
        var predicateLinks = query.getCuratedPredicateLinks();

        var esResponse = (Map<String, Object>) whelk.elastic.query(query.getEsQueryDsl());
        return query.getPartialCollectionView(esResponse, predicateLinks);
    }

    public Map<String, Object> buildStatsReprFromSliceSpec(List<Map<String, Object>> sliceList) {
        Map<String, Object> statsfind = new LinkedHashMap<>();
        for (Map<String, Object> slice : sliceList) {
            String key = (String) ((List<?>) slice.get("dimensionChain")).getFirst();
            int limit = (Integer) slice.get("itemLimit");
            Boolean range = (Boolean) slice.get("range");
            var m = new HashMap<>();
            m.put("sort", "value");
            m.put("sortOrder", "desc");
            m.put("size", limit);
            if (range != null) {
                m.put("range", range);
            }
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
        private final XLQLQuery.StatsRepr statsRepr;
        private final List<String> debug;
        private final String queryString;
        private final String freeText;
        private final String lens;
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
            this.lens = getOptionalSingleNonEmpty(P.LENS, queryParameters).orElse("cards");
            this.statsRepr = getStatsRepr(queryParameters);

            var q = getOptionalSingleNonEmpty(P.QUERY, queryParameters);
            var i = getOptionalSingleNonEmpty(P.SIMPLE_FREETEXT, queryParameters);

            if (q.isPresent()) {
                var sqt = xlqlQuery.normalizeFilters(xlqlQuery.getSimpleQueryTree(q.get(), statsRepr.aliasedFilters), statsRepr);
                if (i.isEmpty() || xlqlQuery.getSimpleQueryTree(i.get(), statsRepr.aliasedFilters).isFreeText()) {
                    this.simpleQueryTree = sqt;
                    this.queryString = xlqlQuery.sqtToQueryString(sqt);
                    this.freeText = sqt.getFreeTextPart();
                    SimpleQueryTree filteredTree = getFilteredTree();
                    this.outsetType = xlqlQuery.getOutsetType(filteredTree);
                    this.queryTree = xlqlQuery.getQueryTree(filteredTree, outsetType);
                    this.esQueryDsl = getEsQueryDsl();
                } else {
                    throw new Crud.RedirectException(makeFindUrl(sqt.getFreeTextPart(), xlqlQuery.sqtToQueryString(sqt)));
                }
            } else if (object != null) {
                throw new Crud.RedirectException(makeFindUrl("", "*"));
            } else {
                throw new InvalidQueryException("Missing required query parameter: _q");
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
            queryDsl.put("aggs", xlqlQuery.getAggQuery(statsRepr.statsRepr, outsetType));
            queryDsl.put("track_total_hits", true);
            return queryDsl;
        }

        // TODO naming things "curated predicate links" ??
        private List<Map<?, ?>> getCuratedPredicateLinks() {
            var o = getObject();
            if (o == null) {
                return Collections.emptyList();
            }
            var esResponse = (Map<String, Object>) whelk.elastic.query(getCuratedPredicateEsQueryDsl(o));
            return xlqlQuery.predicateLinks(esResponse, o, getNonQueryParams(0));
        }

        public Map<String, Object> getCuratedPredicateEsQueryDsl(XLQLQuery.Entity o) {
            var queryDsl = new LinkedHashMap<String, Object>();
            queryDsl.put("query", xlqlQuery.getEsQuery(xlqlQuery.getQueryTree(new SimpleQueryTree(SimpleQueryTree.pvEqualsLiteral("_links", o.id())), outsetType)));
            queryDsl.put("size", 0);
            queryDsl.put("from", 0);
            queryDsl.put("aggs", xlqlQuery.pAgg(o, outsetType));
            queryDsl.put("track_total_hits", true);
            return queryDsl;
        }

        private XLQLQuery.Entity getObject() {
            if (object != null) {
                @SuppressWarnings("unchecked")
                List<String> types = (List<String>) JsonLd.asList(DocumentUtil.getAtPath(whelk.loadData(object), List.of(JsonLd.GRAPH_KEY, 1, JsonLd.TYPE_KEY), null));
                if (!types.isEmpty()) {
                    return new XLQLQuery.Entity(object, types.getFirst());
                }
            }
            return null;
        }

        public Map<String, Object> getPartialCollectionView(Map<String, Object> esResponse, List<Map<?, ?>> predicateLinks) {
            int numHits = (int) esResponse.getOrDefault("totalHits", 0);
            var aliases = XLQLQuery.getAliasMappings(outsetType);
            var view = new LinkedHashMap<String, Object>();
            view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
            view.put(JsonLd.ID_KEY, makeFindUrl(freeText, queryString, offset));
            view.put("itemOffset", offset);
            view.put("itemsPerPage", limit);
            view.put("totalItems", numHits);
            view.put("search", Map.of("mapping", toMappings(aliases, statsRepr.availableBoolFilters)));
            view.putAll(makePaginationLinks(numHits));
            if (esResponse.containsKey("items")) {
                @SuppressWarnings("unchecked")
                var esItems = ((List<Map<String, ?>>) esResponse.get("items"));
                view.put("items", esItems.stream().map(this::shapeResultItem).toList());
            }
            var stats = new HashMap<>(xlqlQuery.getStats(esResponse, statsRepr, simpleQueryTree, getNonQueryParams(0), aliases));
            // TODO naming things
            stats.put("_predicates", predicateLinks);
            view.put("stats", stats);

            if (debug.contains(Debug.ES_QUERY)) {
                view.put(P.DEBUG, Map.of(Debug.ES_QUERY, esQueryDsl));
            }
            view.put("maxItems", whelk.elastic.maxResultWindow);

            return view;
        }

        private SimpleQueryTree getFilteredTree() {
            var filters = new ArrayList<SimpleQueryTree.PropertyValue>();
            if (object == null) {
                filters.addAll(statsRepr.siteDefaultFilters);
            }
            if (object != null) {
                if (predicates.isEmpty()) {
                    filters.add(SimpleQueryTree.pvEqualsLiteral("_links", object));
                } else {
                    filters.addAll(predicates.stream().map(p -> SimpleQueryTree.pvEqualsLink(p, object)).toList());
                }
            }
            return xlqlQuery.addDefaultFilters(simpleQueryTree, filters).expandActiveBoolFilters();
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

        private List<Map<?, ?>> toMappings(Map<String, String> aliases, List<Map<String, Object>> filters) {
            return List.of(xlqlQuery.toMappings(simpleQueryTree, aliases, filters, makeNonQueryParams(0)));
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

        private XLQLQuery.StatsRepr getStatsRepr(Map<String, String[]> queryParameters) throws IOException, InvalidQueryException {
            Map<String, Object> statsRepr = new LinkedHashMap<>();

            var statsJson = Optional.ofNullable(queryParameters.get(P.STATS_REPRESENTATION))
                    .map(x -> x[0])
                    .orElse("{}");

            Map<?, ?> statsMap = mapper.readValue(statsJson, LinkedHashMap.class);
            for (var entry : statsMap.entrySet()) {
                statsRepr.put((String) entry.getKey(), entry.getValue());
            }

            return xlqlQuery.new StatsRepr(statsRepr);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private <V> Map<String, V> shapeResultItem(Map<String, V> esItem) {
            var item = applyLens(esItem, lens, object);

            // ISNIs and ORCIDs are indexed with and without spaces, remove the one with spaces.
            List<Map> identifiedBy = (List<Map>) item.get("identifiedBy");
            if (identifiedBy != null) {
                Function<Object, String> toStr = s -> s != null ? s.toString() : "";
                identifiedBy.removeIf(id -> (Document.isIsni(id) || Document.isOrcid(id))
                        && toStr.apply(id.get("value")).length() == 16 + 3);
            }

            // reverseLinks must be re-added because they might get filtered out in applyLens().
            if (esItem.containsKey("reverseLinks")) {
                Map r = (Map) esItem.get("reverseLinks");
                r.put(JsonLd.ID_KEY, makeFindOLink((String) esItem.get(JsonLd.ID_KEY)));
                item.put("reverseLinks", (V) r);
            }

            return item;
        }

        @SuppressWarnings("unchecked")
        private <V> Map<String, V> applyLens(Map<String, V> framedThing, String lens, @Nullable String preserveId) {
            @SuppressWarnings("rawtypes")
            List<List> preservedPaths = preserveId != null ? JsonLd.findPaths(framedThing, "@id", preserveId) : Collections.emptyList();

            return switch (lens) {
                case "chips" -> (Map<String, V>) whelk.getJsonld().toChip(framedThing, preservedPaths);
                case "full" -> removeSystemInternalProperties(framedThing);
                default -> whelk.getJsonld().toCard(framedThing, false, false, false, preservedPaths, true);
            };
        }

        private String makeFindOLink(String iri) {
            return Document.getBASE_URI()
                    .resolve("find?o=" + URLEncoder.encode(iri, StandardCharsets.UTF_8))
                    .toString();
        }

        private <V> Map<String, V> removeSystemInternalProperties(Map<String, V> framedThing) {
            DocumentUtil.traverse(framedThing, (value, path) -> {
                if (!path.isEmpty() && ((String) path.getLast()).startsWith("_")) {
                    return new DocumentUtil.Remove();
                }
                return DocumentUtil.NOP;
            });
            return framedThing;
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
