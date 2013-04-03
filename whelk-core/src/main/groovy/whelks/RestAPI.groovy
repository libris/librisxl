package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*

import org.json.simple.*

import se.kb.libris.conch.Tools
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
        return "/" + this.whelk.prefix + "/discovery"
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
class RootRouteRestlet extends BasicWhelkAPI {

    String description = "Whelk root routing API"

    RootRouteRestlet(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    String getPath() {
        return "/" + this.whelk.prefix + "/"
    }

    @Override
    def void handle(Request request, Response response) {
        def discoveryAPI
        def documentAPI
        if (request.method == Method.GET) {
            discoveryAPI = new DiscoveryAPI(this.whelk)
            def uri = new URI(discoveryAPI.path)
            log.info "RootRoute API handling route to ${uri} ..."
            discoveryAPI.handle(request, response)
        } else if (request.method == Method.PUT) {
            documentAPI = new DocumentRestlet(this.whelk)
            documentAPI.handle(request, response)
        } else if (request.method == Method.POST) {
            documentAPI = new DocumentRestlet(this.whelk)
            try {
                def identifier
                Document doc = null
                def headers = request.attributes.get("org.restlet.http.headers")
                log.trace("headers: $headers")
                def format = headers.find { it.name.equalsIgnoreCase("Format") }?.value
                log.debug("format: $format")
                log.debug("request: $request")
                def link = headers.find { it.name.equals("link") }?.value
                doc = this.whelk.createDocument(request.entityAsText, ["contentType":request.entity.mediaType.toString(),"format":format])
                //doc = this.whelk.createDocument().withContentType(request.entity.mediaType.toString()).withSize(request.entity.size).withData(request.entity.stream.getBytes())
                if (link != null) {
                    log.trace("Adding link $link to document...")
                    doc = doc.withLink(link)
                }
                identifier = this.whelk.store(doc)
                response.setEntity(doc.dataAsString, new MediaType(doc.contentType))
                response.entity.setTag(new Tag(doc.timestamp as String, false))
                log.debug("Saved document $identifier")
                response.setStatus(Status.REDIRECTION_SEE_OTHER, "Thank you! Document ingested with id ${identifier}")
                log.info("Redirecting with location ref " + request.getRootRef().toString() + identifier)
                response.setLocationRef(request.getRootRef().toString() + "${identifier}")
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
    }
}

@Log
class DocumentRestlet extends BasicWhelkAPI {

    def pathEnd = "{identifier}"
    def varPath = true

    String description = "A GET request with identifier loads a document. A PUT request stores a document. A DELETE request deletes a document."

    DocumentRestlet(Whelk whelk) {
        this.whelk = whelk
    }

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
        log.debug "Path: $path"
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
                    response.entity.setTag(new Tag(d.timestamp as String, false))
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
                    doc = this.whelk.createDocument().withContentType(request.entity.mediaType.toString()).withSize(request.entity.size).withData(request.entity.stream.getBytes())
                } else {
                    //doc = this.whelk.createDocument().withIdentifier(new URI(path)).withContentType(request.entity.mediaType.toString()).withFormat(format).withData(request.entityAsText)
                    doc = this.whelk.createDocument(request.entityAsText, ["identifier":new URI(path),"contentType":request.entity.mediaType.toString(),"format":format])
                }
                if (link != null) {
                    log.trace("Adding link $link to document...")
                    doc = doc.withLink(link)
                }
                // Check If-Match
                def ifMatch = headers.find { it.name.equalsIgnoreCase("If-Match") }?.value
                if (ifMatch
                        && this.whelk.get(new URI(path))
                        && this.whelk.get(new URI(path))?.timestamp as String != ifMatch) {
                    response.setStatus(Status.CLIENT_ERROR_PRECONDITION_FAILED, "The resource has been updated by someone else. Please refetch.")
                } else {
                    identifier = this.whelk.store(doc)
                    //response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
                    response.setStatus(Status.REDIRECTION_SEE_OTHER, "Thank you! Document ingested with id ${identifier}")
                    response.setLocationRef(request.getOriginalRef().toString())
                }
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
        else if (request.method == Method.DELETE) {
            try {
                whelk.delete(new URI(path))
                response.setStatus(Status.SUCCESS_NO_CONTENT)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.SERVER_ERROR_INTERNAL, wre.message)
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
    String defaultBoost = "about.instanceOf.title:5,about.isbn:5,about.instanceOf.authorList.authoritativeName:5,about.instanceOf.subject.authoritativeName:2,about.publisher.name:2"
    //defaultBoost = "fields.020.subfields.a:5,fields.022.subfields.a:5,fields.100.subfields.a:5,fields.100.subfields.b:5,labels.title:3,fields.600.subfields.a:2,fields.600.subfields.b:2,fields.260.subfields.c:2,fields.008.subfields.yearTime1:2"

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
        if (!reqMap["boost"]) {
            reqMap["boost"] = defaultBoost
        }
        try {
            q = new Query(reqMap)
            if (reqMap["f"]) {
                q.query = (q.query + " " + reqMap["f"]).trim()
            }
            def callback = reqMap.get("callback")
            if (q) {
                q.addFacet("about.@type")
                q.addFacet("about.dateOfPublication")
                /*
                q.addFacet("status")
                q.addFacet("typeOfRecord")
                q.addFacet("bibLevel")
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
            def headers = request.attributes.get("org.restlet.http.headers")
            def remote_ip = headers.find { it.name.equalsIgnoreCase("X-Forwarded-For") }?.value
            log.info("Query [" + q?.query + "] completed for $remote_ip in " + (System.currentTimeMillis() - startTime) + " milliseconds and resulted in " + results?.numberOfHits + " hits.")
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

            response.setEntity(c.toJson(), MediaType.APPLICATION_JSON)
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
            def data = mapper.readValue(it.dataAsString, Map)
            data.remove("unknown")
            def doc = ["data":data]
            doc.identifier = it.identifier
            if (it.identifier.toString().contains("/auth/")) {
                doc["authorized"] = true
            }
            list << doc
            /*
            if (it.identifier.toString().contains("/bib/")) {
                list << doc
                //list << mapBibRecord(it.identifier, doc)
            }
            */
        }
        out["hits"] = results.numberOfHits
        out["list"] = list
        return mapper.writeValueAsString(out)
    }

    /*
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
        if (!result && (lastkey == key)) {
            result = findNestedValueForKey(map, key)
        }
        return ((result && result instanceof List && result.size() == 1) ? result[0] : result)
    }

    def findNestedValueForKey(Map map, String key) {
        def result
        map.each { k, v ->
            if (k == key) {
                result = v
            } else if (!result && v instanceof Map) {
                result = findNestedValueForKey(v, key)
            } else if (!result && v instanceof List) {
                v.each {
                    if (it instanceof Map) {
                        result = findNestedValueForKey(it, key)
                    }
                }
            }
        }
        return result
    }
    */

    def mapAuthRecord(id, r) {
        def name = [:]
        name["identifier"] = id
        log.debug("hl : ${r.highlight} (${r.highlight.getClass().getName()})")
        boolean mainhit = r.highlight.any { it.key in mainFields }
        log.debug("mainFields: $mainFields")
        name[mainFields[0]] = Tools.getDeepValue(r, mainFields[0])
        if (!mainhit) {
            name["found_in"] = r.highlight.findAll { !(it.key in mainFields) }//.collect { it.value }[0]
        }
        for (field in supplementalFields) {
            def dv = Tools.getDeepValue(r, field)
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
        name[mainFields[0]] = Tools.getDeepValue(r, mainFields[0])
        log.debug("highlight: ${r.highlight}")
        for (field in supplementalFields) {
            def dv = Tools.getDeepValue(r, field)
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
        "country": "countrycodes.json",
        "nationality": "nationalitycodes.json",
        "relator": "relatorcodes.json",
        "typedef": "typedefs.json"
    ]
    def lang
    def country
    def nationality
    def relator
    def typedef
    def mapper

    ResourceListRestlet() {
        mapper = new ObjectMapper()
        codeFiles.each { key, fileName ->
           log.debug("Load codes $key")
           this[(key)] = loadCodes(key)
        }
    }

    //TODO:implement get and post/put to storage alt. documentrestlet

    def loadCodes(String typeOfCode) {
        try {
            return this.getClass().getClassLoader().getResourceAsStream(codeFiles.get(typeOfCode)).withStream {
                mapper.readValue(it, Map)
            }
        } catch (org.codehaus.jackson.map.JsonMappingException jme) {
            return this.getClass().getClassLoader().getResourceAsStream(codeFiles.get(typeOfCode)).withStream {
                mapper.readValue(it, List).collectEntries { [it.code, it] }   
            }
        }
    }

    def void handle(Request request, Response response) {
        def foundParam = false
        def queryMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        codeFiles.each { key, value ->
            if (queryMap.get(key) && queryMap.get(key).trim().equals("all")) {
                response.setEntity(mapper.writeValueAsString(this[(key)]), MediaType.APPLICATION_JSON)
                foundParam = true
            }
        }
        if (!foundParam) {
            response.setEntity(/{"Error": "Use parameter \"lang=all\", \"country=all\", \"nationality=all\" or \"relator=all\"."}/, MediaType.APPLICATION_JSON)
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
        def obj = marcmap
        def partPath = request.resourceRef.queryAsForm.valuesMap["part"]
        if (partPath) {
            for (key in partPath.split(/\./)) {
                obj = obj[key]
            }
        }
        response.setEntity(mapper.writeValueAsString(obj), MediaType.APPLICATION_JSON)
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
        def link = queryMap.get("link", null)
        def tag = queryMap.get("tag", null)
        def queryObj
        //def queryStr
        def results
        def outjson = new StringBuilder()
        def records = []

        if (link && tag) {
            queryObj = new ElasticQuery("annotates.@id:${link} AND ${tag}")
        } else if (link) {
            queryObj = new ElasticQuery("annotates.@id:${link}")
        } else if (tag) {
            queryObj = new ElasticQuery(tag)
        }

        if (queryObj) {
            queryObj.indexType = "record"
            results = this.whelk.query(queryObj)

            results.hits.each {
               def identifier = it.identifier.toString()
               def d = whelk.get(new URI(identifier))
               records << d
            }

            /*if (link && tag) {
                records.eachWithIndex() { doc, i ->
                    def jsonData = doc.getDataAsJson()
                    log.info("Location " + doc.getDataAsJson()?.get("location"))
                    if (jsonData?.get("location") && !jsonData.get("location").equals(tag.trim())) {
                        records.remove(i)
                    }
                }
            }*/

            outjson << "{ \"list\": ["
            records.eachWithIndex() { it, i ->
                if (i > 0) {
                    outjson << ","
                }
                outjson << it.dataAsString
            }
            outjson << "] }"
            response.setEntity(outjson.toString(), MediaType.APPLICATION_JSON)

        } else {
            response.setEntity('{"Error":"Use parameter \"link=<uri>\". and/or \"tag=location:<sigel>\"}', MediaType.APPLICATION_JSON)
        }
    }
}

