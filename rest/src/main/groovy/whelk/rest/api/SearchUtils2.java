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
        AppParams appParams = new AppParams(getAppConfig(queryParameters), whelk.getJsonld());

        Query query = Query.init(queryParams, appParams, vocabMappings, esSettings, whelk);

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

        Stream.of("statistics", "filterAliases", "defaultSiteFilters", "optionalSiteFilters", "relationFilters")
                .forEach(key ->
                        Optional.ofNullable(findDesc.get(key))
                                .ifPresent(filters -> config.put(key, filters))
                );

        return config;
    }
}
