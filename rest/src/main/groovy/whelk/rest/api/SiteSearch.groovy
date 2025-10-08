package whelk.rest.api

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log

import whelk.JsonLd
import whelk.Whelk
import whelk.exception.InvalidQueryException

import static whelk.JsonLd.findInData
import static whelk.util.Jackson.mapper

@Log
@CompileStatic
class SiteSearch {

    Whelk whelk
    SearchUtils search
    SearchUtils2 search2

    protected Map<String, Map> appsIndex = [:]
    protected Map<String, String> siteAlias = [:]
    protected Map<String, Map> searchStatsReprs = [:]

    SiteSearch(Whelk whelk) {
        this.whelk = whelk
        search = new SearchUtils(whelk)
        setupApplicationSearchData()

        search2 = new SearchUtils2(whelk)
    }

    String getDefaultSite() {
        return whelk.applicationId
    }

    protected synchronized void setupApplicationSearchData() {
        for (var app : whelk.namedApplications.values()) {
            siteAlias[app.alias] = app.id
            // Workaround for complicated path from webserver/webapp to REST API;
            // somehow local request appears as HTTPS even when requested over HTTP...
            if (app.alias.startsWith('http:')) {
                siteAlias['https' + app.alias.substring(4)] = app.id
            }
        }

        var appIds = [whelk.applicationId]
        appIds += whelk.namedApplications.keySet() as List<String>

        appIds.each { appId ->
            var appDesc = getAndIndexDescription(appId)
            if (appDesc) {
                var findDesc = getAndIndexDescription("${appId}find")
                var dataDesc = getAndIndexDescription("${appId}data")
                searchStatsReprs[appId] = [
                    (JsonLd.ID_KEY): appId,
                    'statsfind': buildStatsReprFromSliceSpec(findDesc),
                    'statsindex': buildStatsReprFromSliceSpec(dataDesc),
                    'domain': appId.replaceAll('^https?://([^/]+)/', '$1')
                ]
            }
        }
    }

    protected Map getAndIndexDescription(String id) {
        var data = whelk.loadData(id)
        if (data) {
            var desc = findInData(data, id)
            if (desc) {
                appsIndex[id] = desc
                return desc
            }
        }
        return null
    }

    boolean isSearchResource(String path) {
        return path == "/find" || path.startsWith("/find.") ||
               path == "/data" || path.startsWith("/data.")
    }

    protected String determineActiveSite(Map queryParameters, String baseUri) {
        // If ?_site=<foo> has been specified (and <foo> is a valid site) it takes precedence
        var paramSite = (String) queryParameters["_site"]
        if (paramSite in searchStatsReprs) {
            log.debug("Active site set by _site request parameter: ${paramSite}")
            return paramSite
        }
        var activeSite = defaultSite
        if (baseUri in siteAlias) {
            activeSite = siteAlias[baseUri]
        }
        log.debug("Active site: ${activeSite}")
        return activeSite
    }

    Map findData(Map queryParameters, String baseUri, String path) throws InvalidQueryException {
        var activeSite = determineActiveSite(queryParameters, baseUri)

        var searchSettings = (Map) searchStatsReprs[activeSite]

        // Depending on what site/client we're serving, we might need to add extra query parameters
        // before they're sent further.
        if (activeSite != defaultSite) {
            queryParameters.put('_site_base_uri', [activeSite] as String[])
            if (searchSettings['domain']) {
                queryParameters.put('_boost', [searchSettings['domain']] as String[])
            }
        }
        if (path == "/data" || path.startsWith("/data.")) {
            if (!queryParameters['_statsrepr'] && searchSettings['statsindex']) {
                queryParameters.put('_statsrepr', [mapper.writeValueAsString(searchSettings['statsindex'])] as String[])
            }
            return toDataIndexDescription(appsIndex["${activeSite}data" as String], queryParameters)
        } else if ("_q" in queryParameters || "_o" in queryParameters || "_r" in queryParameters) {
            var appId = "https://beta.libris.kb.se/"
            var appDesc = getAndIndexDescription(appId)
            if (appDesc) {
                var findDesc = getAndIndexDescription("${appId}find")
                queryParameters.put('_appConfig', [mapper.writeValueAsString(search2.buildAppConfig(findDesc))] as String[])
            }
            return search2.doSearch(queryParameters)
        } else {
            if (!queryParameters['_statsrepr'] && searchSettings['statsfind']) {
                queryParameters.put('_statsrepr', [mapper.writeValueAsString(searchSettings['statsfind'])] as String[])
            }
            return search.doSearch(queryParameters)
        }
    }

    Map toDataIndexDescription(Map appDesc, Map queryParameters) {
        if (!queryParameters['_limit']) {
            queryParameters.put('_limit', ["0"] as String[])
        }
        if (!queryParameters['q']) {
            queryParameters.put('q', ["*"] as String[])
        }
        var searchResults = search.doSearch(queryParameters)

        var results = [:]
        results.putAll(appDesc)
        results['statistics'] = searchResults['stats']

        return results
    }

    protected Map buildStatsReprFromSliceSpec(Map desc) {
        var stats = (Map) desc.get('statistics')
        var sliceList = (List) stats?.get('sliceList')
        return sliceList ? search.buildStatsReprFromSliceSpec(sliceList) : null
    }
}
