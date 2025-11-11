package whelk.rest.api;

import com.google.common.io.Files;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.findInData;
import static whelk.util.Jackson.mapper;

class SiteSearch {
    private static final Logger log = LogManager.getLogger(SiteSearch.class);

    Whelk whelk;
    SearchUtils search;
    SearchUtils2 search2;

    protected Map<String, Map> appsIndex = new HashMap<>();
    protected Map<String, String> siteAlias = new HashMap<>();
    protected Map<String, Map> searchStatsReprs = new HashMap<>();

    SiteSearch(Whelk whelk) {
        this.whelk = whelk;
        search = new SearchUtils(whelk);
        setupApplicationSearchData();

        search2 = new SearchUtils2(whelk);
    }

    String getDefaultSite() {
        return whelk.getApplicationId();
    }

    protected synchronized void setupApplicationSearchData() {
        for (Map.Entry<String, ?> entry : whelk.getNamedApplications().entrySet()) {
            Object app = entry.getValue();
            Map<String, Object> appMap = (Map<String, Object>) app;
            String alias = (String) appMap.get("alias");
            String id = (String) appMap.get("id");
            siteAlias.put(alias, id);
            // Workaround for complicated path from webserver/webapp to REST API;
            // somehow local request appears as HTTPS even when requested over HTTP...
            if (alias.startsWith("http:")) {
                siteAlias.put("https" + alias.substring(4), id);
            }
        }

        List<String> appIds = new java.util.ArrayList<>();
        appIds.add(whelk.getApplicationId());
        appIds.addAll(whelk.getNamedApplications().keySet());

        for (String appId : appIds) {
            var appDesc = getAndIndexDescription(appId);
            if (appDesc != null) {
                var findDesc = getAndIndexDescription(appId + "find");
                var dataDesc = getAndIndexDescription(appId + "data");
                searchStatsReprs.put(appId, Map.of(
                        JsonLd.ID_KEY, appId,
                        "statsfind", buildStatsReprFromSliceSpec(findDesc),
                        "statsindex", buildStatsReprFromSliceSpec(dataDesc),
                        "domain", appId.replaceAll("^https?://([^/]+)/", "$1")
                ));
            }
        }
    }

    protected Map<?, ?> getAndIndexDescription(String id) {
        Map<?, ?> data;

        var appsOverride = System.getProperty("xl.test.apps.jsonld");
        if (appsOverride != null && !appsOverride.isEmpty()) {
            log.info("Using {} for {}", appsOverride, id);
            try {
                data = mapper.readValue(
                        Files.asCharSource(new File(appsOverride), StandardCharsets.UTF_8).read(),
                        Map.class
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            data = whelk.loadData(id);
        }

        if (data != null) {
            var desc = (Map<?, ?>) findInData(data, id);
            if (desc != null) {
                appsIndex.put(id, desc);
                return desc;
            }
        }
        return null;
    }

    boolean isSearchResource(String path) {
        return path.equals("/find") || path.startsWith("/find.") ||
               path.equals("/data") || path.startsWith("/data.");
    }

    protected String determineActiveSite(Map<String, String[]>  queryParameters, String baseUri) {
        // If ?_site=<foo> has been specified (and <foo> is a valid site) it takes precedence
        String paramSite = queryParameters.containsKey("_site") ? queryParameters.get("_site")[0] : "";
        if (searchStatsReprs.containsKey(paramSite)) {
            log.debug("Active site set by _site request parameter: {}", paramSite);
            return paramSite;
        }
        String activeSite = getDefaultSite();
        if (siteAlias.containsKey(baseUri)) {
            activeSite = siteAlias.get(baseUri);
        }
        log.debug("Active site: {}", activeSite);
        return activeSite;
    }

    Map findData(Map<String, String[]> queryParameters, String baseUri, String path) throws InvalidQueryException, IOException {
        String activeSite = determineActiveSite(queryParameters, baseUri);

        Map searchSettings = searchStatsReprs.get(activeSite);

        // Depending on what site/client we're serving, we might need to add extra query parameters
        // before they're sent further.
        if (!activeSite.equals(getDefaultSite())) {
            queryParameters.put("_site_base_uri", new String[]{activeSite});
            if (searchSettings.get("domain") != null) {
                queryParameters.put("_boost", new String[]{(String) searchSettings.get("domain")});
            }
        }
        if (path.equals("/data") || path.startsWith("/data.")) {
            if (queryParameters.get("_statsrepr") == null && searchSettings.get("statsindex") != null) {
                queryParameters.put("_statsrepr", new String[]{mapper.writeValueAsString(searchSettings.get("statsindex"))});
            }
            return toDataIndexDescription(appsIndex.get(activeSite + "data"), queryParameters);
        } else if (queryParameters.containsKey("_q") || queryParameters.containsKey("_o") || queryParameters.containsKey("_r")) {
            String appId = "https://beta.libris.kb.se/";
            Map appDesc = getAndIndexDescription(appId);
            if (appDesc != null) {
                Map findDesc = getAndIndexDescription(appId + "find");
                queryParameters.put("_appConfig", new String[]{mapper.writeValueAsString(search2.buildAppConfig(findDesc))});
            }
            return search2.doSearch(queryParameters);
        } else {
            if (queryParameters.get("_statsrepr") == null && searchSettings.get("statsfind") != null) {
                queryParameters.put("_statsrepr", new String[]{mapper.writeValueAsString(searchSettings.get("statsfind"))});
            }
            return search.doSearch(queryParameters);
        }
    }

    Map toDataIndexDescription(Map appDesc, Map<String, String[]> queryParameters) throws InvalidQueryException, IOException {
        queryParameters.computeIfAbsent("_limit", k -> new String[]{"0"});
        queryParameters.computeIfAbsent("q", k -> new String[]{"*"});
        Map searchResults = search.doSearch(queryParameters);

        Map<String, Object> results = new HashMap<>(appDesc);
        results.put("statistics", searchResults.get("stats"));

        return results;
    }

    protected Map buildStatsReprFromSliceSpec(Map desc) {
        if (desc == null) {
            return null;
        }
        Map stats = (Map) desc.get("statistics");
        if (stats == null) {
            return null;
        }
        List sliceList = (List) stats.get("sliceList");
        return sliceList != null ? search.buildStatsReprFromSliceSpec(sliceList) : null;
    }
}
