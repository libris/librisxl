package whelk.rest.api;

import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.exception.WhelkRuntimeException;


import whelk.search2.AppParams;
import whelk.search2.ESSettings;
import whelk.search2.Query;
import whelk.search2.QueryParams;
import whelk.search2.VocabMappings;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static whelk.util.Jackson.mapper;

public class SearchUtils2 {
    private final VocabMappings vocabMappings;
    private final Whelk whelk;
    private final ESSettings esSettings;

    SearchUtils2(Whelk whelk) {
        this.whelk = whelk;
        this.esSettings = new ESSettings(whelk);
        this.vocabMappings = new VocabMappings(whelk);
    }

    Map<String, Object> doSearch(Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
        if (!esSettings.isConfigured()) {
            throw new WhelkRuntimeException("ElasticSearch not configured.");
        }

        QueryParams queryParams = new QueryParams(queryParameters);
        AppParams appParams = new AppParams(getAppConfig(queryParameters), queryParams);

        Query query = Query.init(queryParams, appParams, vocabMappings, esSettings, whelk);;

        return query.collectResults();
    }

    private Map<String, Object> getAppConfig(Map<String, String[]> queryParameters) throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();

        var statsJson = Optional.ofNullable(queryParameters.get(QueryParams.ApiParams.APP_CONFIG))
                .map(x -> x[0])
                .orElse("{}");

        Map<?, ?> statsMap = mapper.readValue(statsJson, LinkedHashMap.class);
        for (var entry : statsMap.entrySet()) {
            config.put((String) entry.getKey(), entry.getValue());
        }

        return config;
    }

    public Map<String, Object> buildAppConfig(Map<String, Object> findDesc) {
        Map<String, Object> config = new LinkedHashMap<>();

        Optional.ofNullable((Map<?, ?>) findDesc.get("statistics"))
                .map(s -> (List<?>) s.get("sliceList"))
                .map(SearchUtils2::buildStatsReprFromSliceSpec)
                .ifPresent(statsRepr -> config.put("_statsRepr", statsRepr));

        Stream.of("filterAliases", "defaultSiteFilters", "optionalSiteFilters", "relationFilters")
                .forEach(key ->
                        Optional.ofNullable(findDesc.get(key))
                                .ifPresent(filters -> config.put("_" + key, filters))
                );

        return config;
    }

    private static Map<String, Object> buildStatsReprFromSliceSpec(List<?> sliceList) {
        Map<String, Object> statsRepr = new LinkedHashMap<>();
        for (var s : sliceList) {
            var slice = ((Map<?, ?>) s);
            String key = (String) ((List<?>) slice.get("dimensionChain")).getFirst();
            int limit = (Integer) slice.get("itemLimit");
            Boolean range = (Boolean) slice.get("range");
            String connective = (String) slice.get("connective");
            var m = new HashMap<>();
            m.put("sort", "count");
            m.put("sortOrder", "desc");
            m.put("size", limit);
            if (range != null) {
                m.put("range", range);
            }
            if (connective != null) {
                m.put("connective", connective);
            }
            statsRepr.put(key, m);
        }
        return statsRepr;
    }
}
