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
                if (path == "/") {
                    doc = this.whelk.createDocument().withContentType(request.entity.mediaType.toString()).withSize(request.entity.size).withDataAsStream(request.entity.stream)
                } else {
                    doc = this.whelk.createDocument().withIdentifier(new URI(path)).withContentType(request.entity.mediaType.toString()).withData(request.entityAsText)
                }
                identifier = this.whelk.store(doc)
                response.setEntity("Thank you! Document ingested with id ${identifier}\n", MediaType.TEXT_PLAIN)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
    }
}

/*
@Log
class CharacterRestlet extends BasicWhelkAPI {
    def pathEnd = "_chars"
    @Override
    def void handle(Request request, Response response) {
        def reqMap = request.resourceRef.queryAsForm.valuesMap
        String word = reqMap["word"]
        String cword = word
        for (c in word.toCharArray()) {
            if (c == 'ö') {
                log.debug("EY!")
                //log.debug("\\u" + Integer.toHexString('÷' | 0x10000).substring(1))
            }
            log.debug("Character: $c ")
        }
        response.setEntity("WORD: $cword", MediaType.TEXT_PLAIN)
    }
}
*/

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
             "ebook": "leader.subfields.typeOfRecord:a leader.subfields.bibLevel:m fields.007.subfields.carrierType:c fields.007.subfields.computerMaterial:r",
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
        def q
        if (!reqMap["boost"]) {
            reqMap["boost"] = defaultBoost
        }
        try {
            q = new Query(reqMap)
            def callback = reqMap.get("callback")
            if (q) {
                q.addFacet("leader.subfields.typeOfRecord")
                q.addFacet("leader.subfields.bibLevel")
                q.addFacet("fields.007.subfields.carrierType")
                q.addFacet("fields.008.subfields.yearTime1")
                q = addQueryFacets(q)
                q = expandQuery(q)
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
        } finally {
            log.info("Query [" + q?.query + "] completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
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

/*
@Log
class BibSearchRestlet extends BasicWhelkAPI {

    def pathEnd = "_bibsearch"

    @Override
    def void handle(Request request, Response response) {
        def query = request.getResourceRef().getQueryAsForm().getValuesMap()
        boolean _raw = (query['_raw'] == 'true')
        try {
            def r = this.whelk.query(query.get("q"))
            response.setEntity(r.result, MediaType.APPLICATION_JSON)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }
    }
}

@Log 
class ImportRestlet extends BasicWhelkAPI {
    def pathEnd = "_import"

    BatchImport importer
    ImportRestlet() {
        importer = new BatchImport()
    }

    @Override
    def void handle(Request request, Response response) {
        importer.resource = this.whelk.prefix
        def millis = System.currentTimeMillis()
        def count = importer.doImport(this.whelk)
        def diff = (System.currentTimeMillis() - millis)/1000
        response.setEntity("Imported $count records into ${whelk.prefix} in $diff seconds.\n", MediaType.TEXT_PLAIN)
    }
}

@Log
class LogRestlet extends BasicWhelkAPI {
    def pathEnd = "_log"
    @Override
    def void handle(Request request, Response response) {
        def query = request.getResourceRef().getQueryAsForm().getValuesMap()
        def since = query.get("since")
        def date = Date.parse("yyyy-MM-dd HH:mm", since)
        def results = this.whelk.log(date)
        def stringResult = new StringBuilder()
        int count = 0
        results.each {
            count++
            log.debug("${it.identifier} (${it.timestamp})")
            //stringResult << it.identifier << " (" << it.timestamp << ")" << "\n"
        }
        stringResult.insert(0, "Hittade $count uppdaterade dokument sedan $date\n")

        response.setEntity(stringResult.toString(), MediaType.TEXT_PLAIN)
    }
}
*/

@Log 
class AutoComplete extends BasicWhelkAPI implements JSONSerialisable, JSONInitialisable {

    def pathEnd = "_complete"

    def namePrefixes = []
    String description = "Search API for autocompletion. Use parameter name or q."

    /*
    def void addNamePrefix(String prefix) {
        namePrefixes << prefix
    }
    */

    AutoComplete(java.util.ArrayList namePfxs) {
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
