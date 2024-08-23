package whelk.rest.api;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.exception.WhelkRuntimeException;
import whelk.search2.Aggs;
import whelk.search2.AppParams;
import whelk.search2.Disambiguate;
import whelk.search2.Pagination;
import whelk.search2.QueryParams;
import whelk.search2.QueryResult;
import whelk.search2.QueryUtil;
import whelk.search2.Sort;
import whelk.search2.Stats;
import whelk.search2.querytree.QueryTree;

import java.io.IOException;
import java.util.*;

import static whelk.util.Jackson.mapper;

public class SearchUtils2 {
    private final QueryUtil queryUtil;
    private final Disambiguate disambiguate;

    SearchUtils2(Whelk whelk) {
        this.queryUtil = new QueryUtil(whelk);
        this.disambiguate = new Disambiguate(whelk);
    }

    Map<String, Object> doSearch(Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
        if (!queryUtil.esIsConfigured()) {
            throw new WhelkRuntimeException("ElasticSearch not configured.");
        }

        QueryParams queryParams = new QueryParams(queryParameters);

        if (queryParams.q.isEmpty()) {
            if (queryParams.object == null) {
                throw new InvalidQueryException("Missing required query parameter: _q");
            } else {
                throw new Crud.RedirectException(QueryUtil.makeFindUrl("", "*", queryParams.getNonQueryParams()));
            }
        }

        AppParams appParams = getAppParams(queryParameters, disambiguate);

        QueryTree qTree = new QueryTree(queryParams.q, disambiguate, appParams.siteFilters.aliasToFilter())
                .normalizeFilters(appParams.siteFilters);
        QueryTree iTree = new QueryTree(queryParams.i, disambiguate, appParams.siteFilters.aliasToFilter());

        if (!iTree.isEmpty() && !iTree.isFreeText()) {
            throw new Crud.RedirectException(QueryUtil.makeFindUrl(qTree, queryParams.getNonQueryParams()));
        }

        qTree.addFilters(queryParams, appParams);
        qTree.setOutsetType(disambiguate);

        Map<String, Object> esQueryDsl = getEsQueryDsl(qTree, queryParams, appParams.statsRepr);

        QueryResult queryRes = new QueryResult(queryUtil.query(esQueryDsl));

        Map<String, Object> partialCollectionView = getPartialCollectionView(queryRes, qTree, queryParams, appParams);

        if (queryParams.debug.contains(QueryParams.Debug.ES_QUERY)) {
            partialCollectionView.put(QueryParams.ApiParams.DEBUG, Map.of(QueryParams.Debug.ES_QUERY, esQueryDsl));
        }

        return partialCollectionView;
    }

    private Map<String, Object> getEsQueryDsl(QueryTree queryTree, QueryParams queryParams, AppParams.StatsRepr statsRepr) {
        var queryDsl = new LinkedHashMap<String, Object>();
        queryDsl.put("query", queryTree.toEs(queryUtil, disambiguate));
        queryDsl.put("size", queryParams.limit);
        queryDsl.put("from", queryParams.offset);
        queryDsl.put("sort", (queryParams.sortBy == Sort.DEFAULT_BY_RELEVANCY && queryTree.isWild()
                ? Sort.BY_DOC_ID
                : queryParams.sortBy).getSortClauses(queryUtil::getSortField));
        queryDsl.put("aggs", Aggs.buildAggQuery(statsRepr, disambiguate, queryTree.getOutsetType(), queryUtil::getNestedPath));
        queryDsl.put("track_total_hits", true);
        return queryDsl;
    }

    public Map<String, Object> getPartialCollectionView(QueryResult queryResult,
                                                        QueryTree qt,
                                                        QueryParams queryParams,
                                                        AppParams appParams) {
        var fullQuery = qt.toString();
        var freeText = qt.getTopLevelFreeText();
        var view = new LinkedHashMap<String, Object>();

        view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
        view.put(JsonLd.ID_KEY, QueryUtil.makeFindUrl(freeText, fullQuery, queryParams.getNonQueryParams()));
        view.put("itemOffset", queryParams.offset);
        view.put("itemsPerPage", queryParams.limit);
        view.put("totalItems", queryResult.numHits);
        // TODO: Include _o search respresentation in search mapping?
        view.put("search", Map.of("mapping", List.of(qt.toSearchMapping(queryParams.getNonQueryParams(0)))));
        view.putAll(Pagination.makeLinks(queryResult.numHits, queryUtil.maxItems(), freeText, fullQuery, queryParams));
        view.put("items", queryResult.collectItems(queryUtil.getApplyLensFunc(queryParams)));
        view.put("stats", new Stats(disambiguate, queryUtil, qt, queryResult, queryParams, appParams).build());
        view.put("maxItems", queryUtil.maxItems());

        return view;
    }

    private AppParams getAppParams(Map<String, String[]> queryParameters, Disambiguate disambiguate) throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();

        var statsJson = Optional.ofNullable(queryParameters.get(QueryParams.ApiParams.APP_CONFIG))
                .map(x -> x[0])
                .orElse("{}");

        Map<?, ?> statsMap = mapper.readValue(statsJson, LinkedHashMap.class);
        for (var entry : statsMap.entrySet()) {
            config.put((String) entry.getKey(), entry.getValue());
        }

        return new AppParams(config, disambiguate);
    }

    public Map<String, Object> buildAppConfig(Map<String, Object> findDesc) {
        Map<String, Object> config = new LinkedHashMap<>();

        Optional.ofNullable((Map<?, ?>) findDesc.get("statistics"))
                .map(s -> (List<?>) s.get("sliceList"))
                .map(SearchUtils2::buildStatsReprFromSliceSpec)
                .ifPresent(statsRepr -> config.put("_statsRepr", statsRepr));

        Optional.ofNullable(findDesc.get("filterAliases"))
                .ifPresent(filterAliases -> config.put("_filterAliases", filterAliases));

        Optional.ofNullable(findDesc.get("defaultSiteFilters"))
                .ifPresent(defaultSiteFilters -> config.put("_defaultSiteFilters", defaultSiteFilters));

        Optional.ofNullable(findDesc.get("defaultSiteTypeFilters"))
                .ifPresent(defaultSiteFilters -> config.put("_defaultSiteTypeFilters", defaultSiteFilters));

        Optional.ofNullable(findDesc.get("optionalSiteFilters"))
                .ifPresent(optionalSiteFilters -> config.put("_optionalSiteFilters", optionalSiteFilters));

        Optional.ofNullable(findDesc.get("relationFilters"))
                .ifPresent(relationFilters -> config.put("_relationFilters", relationFilters));

        return config;
    }

    private static Map<String, Object> buildStatsReprFromSliceSpec(List<?> sliceList) {
        Map<String, Object> statsRepr = new LinkedHashMap<>();
        for (var s : sliceList) {
            var slice = ((Map<?,?>) s);
            String key = (String) ((List<?>) slice.get("dimensionChain")).getFirst();
            int limit = (Integer) slice.get("itemLimit");
            Boolean range = (Boolean) slice.get("range");
            var m = new HashMap<>();
            m.put("sort", "count");
            m.put("sortOrder", "desc");
            m.put("size", limit);
            if (range != null) {
                m.put("range", range);
            }
            statsRepr.put(key, m);
        }
        return statsRepr;
    }
}
