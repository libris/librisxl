package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*

import org.json.simple.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.imports.*
import se.kb.libris.whelks.persistance.*

import org.codehaus.jackson.map.*

interface RestAPI extends API {
    String getPath()
}

@Log
abstract class BasicWhelkAPI extends Restlet implements RestAPI {
    Whelk whelk

    def pathEnd = ""
    def varPath = false
    boolean enabled = true

    String id = "RestAPI"
    int order = 0

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    def void setWhelk(Whelk w) {
        this.whelk = w 
    }
    @Override
    void init(String w) {}

    String getPath() {
        return "/" + this.whelk.prefix + "/" + getPathEnd()
    }

    /**
     * For discovery API.
     */
    abstract String getDescription();

    private String getModifiedPathEnd() {
        if (getPathEnd() == null || getPathEnd().length() == 0) {
            return ""
        } else {
            return "/" + getPathEnd()
        }
    }
    @Override
    int compareTo(Plugin p) {
        return (this.getOrder() - p.getOrder());
    }
}

@Log
class DiscoveryAPI extends BasicWhelkAPI {

    ObjectMapper mapper = new ObjectMapper()

    String description = "Discovery API"

    DiscoveryAPI(Whelk w) {
        this.whelk = w
    }

    @Override
    String getPath() {
        return "/" + this.whelk.prefix + "/"
    }

    @Override
    def void handle(Request request, Response response) {
        def info = [:]
        info["whelk"] = whelk.prefix
        info["apis"] = whelk.getAPIs().collect {
            [ [ "path" : (it.path) ,
                "description" : (it.description) ]
            ] }
        log.debug("info: $info")

        response.setEntity(mapper.writeValueAsString(info), MediaType.APPLICATION_JSON)
    }
}

enum DisplayMode {
    DOCUMENT, META
}

@Log
class DocumentRestlet extends BasicWhelkAPI {

    def pathEnd = "{identifier}"
    def varPath = true

    String description = "A GET request with identifier loads a document. A PUT request stores a document."

    def _escape_regex(str) {
        def escaped = new StringBuffer()
        str.each {
            if (it == "{" || it == "}") {
                escaped << "\\"
            }
            escaped << it 
        }
        return escaped.toString()
    }

    def determineDisplayMode(path) {
        if (path.endsWith("/meta")) {
            return [path[0 .. -6], DisplayMode.META]
        }
        return [path, DisplayMode.DOCUMENT]
    }

    def void handle(Request request, Response response) {
        log.debug("reqattr path: " + request.attributes["identifier"])
        String path = path.replaceAll(_escape_regex(pathEnd), request.attributes["identifier"])
        def mode = DisplayMode.DOCUMENT
        (path, mode) = determineDisplayMode(path)
        boolean _raw = (request.getResourceRef().getQueryAsForm().getValuesMap()['_raw'] == 'true')
        if (request.method == Method.GET) {
            log.debug "Request path: ${path}"
            log.debug " DisplayMode: $mode"
            try {
                def d = whelk.get(new URI(path))
                log.debug("Document received from whelk: $d")
                if (d) {
                    if (mode == DisplayMode.META) {
                        response.setEntity(d.toJson(), MediaType.APPLICATION_JSON)
                    } else {
                        response.setEntity(d.dataAsString, new MediaType(d.contentType))
                    }
                } else {
                    log.debug("Failed to find a document with URI $path")
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND)
                }
            } catch (WhelkRuntimeException wrte) {
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
            }
        }
        else if (request.method == Method.PUT || request.method == Method.POST) {
            try {
                def identifier
                Document doc = null
                def headers = request.attributes.get("org.restlet.http.headers")
                log.trace("headers: $headers")
                def format = headers.find { it.name.equalsIgnoreCase("Format") }?.value
                log.debug("format: $format")
                def link = headers.find { it.name.equals("link") }?.value
                if (path == "/") {
                    doc = this.whelk.createDocument().withContentType(request.entity.mediaType.toString()).withSize(request.entity.size).withDataAsStream(request.entity.stream)
                } else {
                    doc = this.whelk.createDocument().withIdentifier(new URI(path)).withContentType(request.entity.mediaType.toString()).withFormat(format).withData(request.entityAsText)
                }
                if (link != null) {
                    log.trace("Adding link $link to document...")
                    doc = doc.withLink(link)
                }
                identifier = this.whelk.store(doc)
                response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
    }
}

@Log
class SearchRestlet extends BasicWhelkAPI {

    def pathEnd = "_find"
    def defaultQueryParams = [:]

    String description = "Generic search API, acception both GET and POST requests. Accepts parameters compatible with the Query object. (Simple usage: ?q=searchterm)"


    SearchRestlet(Map queryParams) {
        this.defaultQueryParams = queryParams
    }

    @Override
    def void handle(Request request, Response response) {
        log.debug "SearchRestlet with path $path"
        def reqMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        try {
            this.defaultQueryParams.each { key, value ->
                if (!reqMap[key]) {
                    reqMap[key] = value
                }
            }
            log.debug("reqMap: $reqMap")
            def query = new Query(reqMap)
            def callback = reqMap.get("callback")
            def results = this.whelk.query(query)
            def jsonResult =
                (callback ? callback + "(" : "") +
                results.toJson() +
                (callback ? ");" : "")

            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wrte.message)
        }
    }
}

@Log
class KitinSearchRestlet2 extends BasicWhelkAPI {
    def pathEnd = "kitin/_search"

    String description = "Query API with preconfigured parameters for Kitin."

    //String defaultBoost = "labels.title:2,labels.author:2,labels.isbn:2"
    String defaultBoost = "fields.020.subfields.a:5,fields.022.subfields.a:5,fields.100.subfields.a:5,fields.100.subfields.b:5,labels.title:3,fields.600.subfields.a:2,fields.600.subfields.b:2,fields.260.subfields.c:2,fields.008.subfields.yearTime1:2"

    def queryFacets = [
        "custom.bookSerial": [
             //"ebook": "leader.subfields.typeOfRecord:a leader.subfields.bibLevel:m fields.007.subfields.carrierType:c fields.007.subfields.computerMaterial:r",
             "ebook": "about.marc:typeOfRecord.code:a about.instanceOf.marc:bibLevel.code:m fields.007.subfields.carrierType:c fields.007.subfields.computerMaterial:r",
             "audiobook": "leader.subfields.typeOfRecord:i leader.subfields.bibLevel:m fields.007.subfields.carrierType:s",
             "eserial": "leader.subfields.typeOfRecord:a leader.subfields.bibLevel:s fields.007.subfields.carrierType:c fields.007.subfields.computerMaterial:r",
             "book": "leader.subfields.typeOfRecord:a leader.subfields.bibLevel:m !fields.007.subfields.carrierType:c",
             "serial": "leader.subfields.typeOfRecord:a leader.subfields.bibLevel:s !fields.007.subfields.carrierType:c"
        ]
    ]


    Query addQueryFacets(Query q) {
        for (facetgroup in queryFacets) {
            facetgroup.value.each {fl, qv ->
                q.addFacet(fl, qv, facetgroup.key)
            }
        }
        return q
    }

    /**
     * Look for, and expand customFacets.
     */
    Query expandQuery(Query q) {
        def newquery = []
        for (queryitem in q.query.split()) {
            if (queryitem.contains(":")) {
                def (group, key) = queryitem.split(":")
                if (queryFacets[group]) {
                    newquery << (queryFacets[group][key] ?: "")
                } else {
                    newquery << queryitem
                }
            } else {
                newquery << queryitem
            }
        }
        q.query = newquery.join(" ")
        return q
    }

    @Override
    def void handle(Request request, Response response) {
        long startTime = System.currentTimeMillis()
        def reqMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def q, results
        /*
        if (!reqMap["boost"]) {
            reqMap["boost"] = defaultBoost
        }
        */
        try {
            q = new Query(reqMap)
            if (reqMap["f"]) {
                q.query = (q.query + " " + reqMap["f"]).trim()
            }
            def callback = reqMap.get("callback")
            if (q) {
                q.addFacet("status.label")
                q.addFacet("about.typeOfRecord.label")
                q.addFacet("about.instanceOf.bibLevel.label")
                /*
                q.addFacet("fields.007.subfields.carrierType")
                q.addFacet("fields.008.subfields.yearTime1")
                */
                //q = addQueryFacets(q)
                q = expandQuery(q)
                results = this.whelk.query(q)
                def jsonResult =
                    (callback ? callback + "(" : "") +
                    results.toJson() +
                    (callback ? ");" : "")

                response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
            } else {
                response.setEntity("Missing q parameter", MediaType.TEXT_PLAIN)
            }
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        } finally {
            log.info("Query [" + q?.query + "] completed in " + (System.currentTimeMillis() - startTime) + " milliseconds and resulted in " + results?.numberOfHits + " hits.")
        }
    }
}

@Log
class KitinSearchRestlet extends BasicWhelkAPI {

    def pathEnd = "kitin/_oldsearch"
    String description = "old crap"

    def facit = [
        "bibid": ["001"],
        "f":     ["100.a","505.r","700.a"],
        "förf":  ["100.a","505.r","700.a"],
        "isbn":  ["020.az"],
        "issn":  ["022.amyz"],
        "t":     ["242.ab","245.ab","246.ab","247.ab","249.ab","740.anp"],
        "tit":   ["242.ab","245.ab","246.ab","247.ab","249.ab","740.anp"],
        "titel": ["242.ab","245.ab","246.ab","247.ab","249.ab","740.anp"]
        ]



    def expandPrefix(query) {
        println "prefixedQuery: $query"
        def prefixedValue = query.split(":")
        println "prefix: " + prefixedValue[0]
        def expFields = []
        println "Facit: $facit"
        if (facit.containsKey(prefixedValue[0])) {
            println "value: " + facit.get(prefixedValue[0])
            facit.get(prefixedValue[0]).each {
                println "it: $it"
                def fs = it.split(/\./)
                println "fs: $fs"
                if (fs.size() > 1) {
                    for (int i = 0; i < fs[1].length(); i++) {
                        println "splitted subfields ${fs[1][i]}"
                        expFields << "fields." + fs[0] + ".subfields." + fs[1][i] + ":" + prefixedValue[1]
                    }
                } else {
                    expFields << "fields." + fs[0] + ":" + prefixedValue[1]
                }
            }
        } else {
            expFields = [ query ]
        }
        println "expFields: $expFields"
        return expFields
    }

    def isIsbn(string) {
        def result = string.trim().replaceAll("-", "").matches("[0-9]{10}|[0-9]{13}")
        println "Result $result"
        return result
    }

    def isIssn(string) {
        return string.trim().matches("[0-9]{4}-?[0-9]{4}")
    }

    def splitQuery(q) {
        q = q.toLowerCase()
        def bits = q.findAll(/([^\s]+:"[^"]+"|[^\s]+:[^\s]+|"[^"]+"|[^\s]+)\s*/)
        def newQuery = []
        def isbnQuery = []
        bits.eachWithIndex() { it, i ->
            it = it.trim()
            println "bit: $it"
            if (isIsbn(it)) {
                it = "isbn:$it"
            }
            if (isIssn(it)) {
                it = "issn:$it"
            }
            if (it.contains(":")) {
                if (it.startsWith("isbn:")) {
                    isbnQuery << expandPrefix(it).join(" OR ")
                } else {
                    newQuery << "(" + expandPrefix(it).join(" OR ") + ")"
                }
            } else {
                newQuery << it 
            }
        }
        def query
        if (newQuery.size() > 0) {
            query = "(" + newQuery.join(" AND ") + ")"
        }
        if (isbnQuery.size() > 0) {
            def isbnq = "(" + isbnQuery.join(" OR ") + ")"
            query = (query ? query + " OR " + isbnq : isbnq)
        }
        println "query: $query"
        return query
    }

    @Override
    def void handle(Request request, Response response) {
        def query = request.getResourceRef().getQueryAsForm().getValuesMap()
        try {
            def q = new Query(splitQuery(query.get("q")))
            def callback = query.get("callback")
            if (q) {
                q.addFacet("målspråk", "fields.041.subfields.a")
                q.addFacet("originalspråk", "fields.041.subfields.h")
                def results = this.whelk.query(q)
                def jsonResult = 
                    (callback ? callback + "(" : "") +
                    results.toJson() +
                    (callback ? ");" : "") 

                response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
            } else {
                response.setEntity("Missing q parameter", MediaType.TEXT_PLAIN)
            }
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }

    }
}
@Log
class AutoComplete extends BasicWhelkAPI {

    def pathEnd = "_complete"

    def namePrefixes = []
    def extraInfo = []
    String types
    String description = "Search API for autocompletion. Use parameter name or q."

    /*
    def void addNamePrefix(String prefix) {
        namePrefixes << prefix
    }
    */

    AutoComplete(Map lists) {
        namePrefixes.addAll(lists.get("queryFields"))
        extraInfo.addAll(lists.get("infoFields"))
        types = lists.get("indexTypes")
    }

    @Override
    def void handle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String name = querymap.get("name")
        if (!name) {
            name = querymap.get("q")
        }
        def callback = querymap.get("callback")
        if (name) {
            if (name[-1] != ' ' && name[-1] != '*') {
                name = name + "*"
            }
            log.debug("name: $name")
            log.debug("namePrefixes: $namePrefixes")
            LinkedHashMap sortby = new LinkedHashMap<String,String>()
            sortby['records'] = "desc"
            def query = new Query(name)
            query.highlights = namePrefixes
            //query.sorting = sortby
            query.fields = namePrefixes

            def results = this.whelk.query(query, types)
            /*
            def jsonResult = 
                (callback ? callback + "(" : "") +
                results.toJson() +
                (callback ? ");" : "") 
                */
            def c = new SuggestResultsConverter(results, [namePrefixes[0]], extraInfo)
            def jsonResult = c.toJson()

            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"error":"Parameter \"name\" is missing."}', MediaType.APPLICATION_JSON)
        }
    }
}

@Log
class SuggestResultsConverter {
    def results
    ObjectMapper mapper
    List mainFields
    List supplementalFields

    SuggestResultsConverter(SearchResult r, List mfs, List sfs) {
        this.results = r
        this.mapper = new ObjectMapper()
        this.mainFields = mfs
        this.supplementalFields = sfs
    }

    String toJson() {
        def list = []
        def out = [:]
        results.hits.each {
            def doc = mapper.readValue(it.dataAsString, Map)
            if (it.identifier.toString().contains("/auth/")) {
                list << mapAuthRecord(it.identifier, doc)
            }
            if (it.identifier.toString().contains("/bib")) {
                list << mapBibRecord(it.identifier, doc)
            }
        }
        out["hits"] = results.numberOfHits
        out["list"] = list
        return mapper.writeValueAsString(out)
    }

    def getDeepValue(Map map, String key) {
        //log.trace("getDeepValue: map = $map, key = $key")
        def keylist = key.split(/\./)
        def lastkey = keylist[keylist.length-1]
        def result
        for (int i = 0; i < keylist.length; i++) {
            def k = keylist[i]
            while (map.containsKey(k)) {
                if (k == lastkey) {
                    result = map.get(k)
                    map = [:]
                } else {
                    if (map.get(k) instanceof Map) {
                        map = map.get(k)
                    } else {
                        result = []
                        for (item in map[k]) {
                            def dv = getDeepValue(item, keylist[i..-1].join("."))
                            if (dv) {
                                result << dv
                            }
                        }
                        map = [:]
                    }
                }
            }
        }
        return ((result && result instanceof List && result.size() == 1) ? result[0] : result)
    }

    def mapAuthRecord(id, r) {
        def name = [:]
        name["identifier"] = id
        log.debug("hl : ${r.highlight} (${r.highlight.getClass().getName()})")
        boolean mainhit = r.highlight.any { it.key in mainFields }
        log.debug("mainFields: $mainFields")
        name["name"] = getDeepValue(r, mainFields[0])
        if (!mainhit) {
            name["found_in"] = r.highlight.findAll { !(it.key in mainFields) }//.collect { it.value }[0]
        }
        for (field in supplementalFields) {
            def dv = getDeepValue(r, field)
            log.trace("dv $field : $dv")
            if (dv) {
                name[field] = dv
            }
        }
        name["authorized"] = true

        return name
    }

    def mapBibRecord(id, r) {
        def name = [:]
        name["identifier"] = id
        name["name"] = r.highlight.collect { it.value }
        log.debug("highlight: ${r.highlight}")
        for (field in supplementalFields) {
            def dv = getDeepValue(r, field)
            log.trace("dv $field : $dv")
            if (dv) {
                name[field] = dv
            }
        }
        return name
    }
}

@Log
class OldAutoComplete extends BasicWhelkAPI implements JSONSerialisable, JSONInitialisable {

    def pathEnd = "_complete"

    def namePrefixes = []
    String description = "Search API for autocompletion. Use parameter name or q."

    /*
    def void addNamePrefix(String prefix) {
        namePrefixes << prefix
    }
    */

    OldAutoComplete(java.util.ArrayList namePfxs) {
        namePrefixes.addAll(namePfxs)
    }

    @Override
    def void handle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String name = querymap.get("name")
        if (!name) {
            name = querymap.get("q")
        }
        def callback = querymap.get("callback")
        if (name) {
            if (name[-1] != ' ' && name[-1] != '*') {
                name = name + "*"
            }
            def names = []
            namePrefixes.each { p ->
                def nameparts = []
                name.split(" ").each {
                    nameparts << "$p:$it"
                }
                names << nameparts.join(" AND ")
            }
            //def query = names.join(" OR ")
            LinkedHashMap sortby = new LinkedHashMap<String,String>()
            //sortby['100.a'] = "asc"
            sortby['records'] = "desc"
            def query = new Query(name)
            query.highlights = namePrefixes
            query.sorting = sortby
            query.fields = namePrefixes

            def results = this.whelk.query(query)
            def jsonResult = 
                (callback ? callback + "(" : "") +
                results.toJson() +
                (callback ? ");" : "") 

            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"error":"Parameter \"name\" is missing."}', MediaType.APPLICATION_JSON)
        }
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        for (Iterator it = obj.get("prefixes").iterator(); it.hasNext();) {
            try {
                def _prefix = it.next();
                namePrefixes << _prefix.toString()
            } catch (Exception e) {
                throw new WhelkRuntimeException(e);
            }
        }
        return this;
    }

    @Override
    JSONObject serialize() {
        JSONObject _api = new JSONObject();
        _api.put("_classname", this.getClass().getName());
        JSONArray _prefixes = new JSONArray();
        namePrefixes.each {
            _prefixes.add(it)
        }
        _api.put("prefixes", _prefixes);
                
        return _api;
    }
}

@Log
class ResourceListRestlet extends BasicWhelkAPI {
    def pathEnd = "_resourcelist"

    String description = "Query API for language and country codes."

    def codeFiles = [
        "lang": "langcodes.json",
        "country": "countrycodes.json"
    ]
    def langcodes
    def countrycodes
    def mapper

    ResourceListRestlet() {
        mapper = new ObjectMapper()
        langcodes = loadCodes("lang")
        countrycodes = loadCodes("country")
    }

    def convertPropertiesToJson(def typeOfCode) {
        def outjson = [:]
        def properties = new Properties()
        properties.load(this.getClass().getClassLoader().getResourceAsStream("$typeOfCode" + "codes.properties"))
        properties.each { key, value ->
            outjson[key] = value
        }
        def file = new File("$typeOfCode" + "codes.json")
        file << mapper.defaultPrettyPrintingWriter().writeValueAsString(outjson).getBytes("utf-8")
    }

    def loadCodes(def typeOfCode) {
        def jsonfile = "$typeOfCode" + "codes.json"
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("$jsonfile")
        return mapper.readValue(is, Map)
    }

    def void handle(Request request, Response response) {
        def queryMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def lang = queryMap.get("lang")
        def country = queryMap.get("country")

        if (lang && lang.trim().equals("all")) {
            response.setEntity(mapper.writeValueAsString(langcodes), MediaType.APPLICATION_JSON)

        } else if (country && country.trim().equals("all")) {
               response.setEntity(mapper.writeValueAsString(countrycodes), MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"error":"Use parameter \"lang=<all/language>\" or \"country=<all/country>\"."}', MediaType.APPLICATION_JSON)
        }


    }
}

@Log
class MarcMapRestlet extends BasicWhelkAPI {
    def pathEnd = "_marcmap"

    String description = "API for marcmap."

    static final String marcmapfile = "marcmap.json"
    def marcmap
    def mapper

    MarcMapRestlet() {
        mapper = new ObjectMapper()
        marcmap = loadMarcMap()
    }

    def loadMarcMap() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("$marcmapfile")
        return mapper.readValue(is, Map)
    }

    def void handle(Request request, Response response) {
        response.setEntity(mapper.writeValueAsString(marcmap), MediaType.APPLICATION_JSON)
    }
}

@Log
class MetadataSearchRestlet extends BasicWhelkAPI {
    def pathEnd = "_metasearch"
    def mapper

    String description = "Query API for metadata search."

    def void handle(Request request, Response response) {
        mapper = new ObjectMapper()
        def queryMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def link = queryMap.get("link")
        def queryStr
        def results
        def outjson = new StringBuilder()
        def records = []
        if (link) {
            queryStr = new Query(link).addField("links.identifier")
            results = this.whelk.query(queryStr, "metadata")
            results.hits.each {
                def identifier = it.identifier.toString()
                def d = whelk.get(new URI(identifier))
                records << d.dataAsString
            }
            outjson << "{ \"list\": ["
            records.eachWithIndex() { it, i ->
                 if (i > 0) {
                     outjson << ","
                 }
                 outjson << it
            }
            outjson << "] }"
            response.setEntity(outjson.toString(), MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"Error":"Use parameter \"link=<uri>\"."}', MediaType.APPLICATION_JSON)
        }
    }
}

