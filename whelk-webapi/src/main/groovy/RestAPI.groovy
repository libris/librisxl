package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.resource.*

import se.kb.libris.conch.Tools
import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.http.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.imports.*
import se.kb.libris.whelks.persistance.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import se.kb.libris.conch.converter.MarcJSONConverter

import se.kb.libris.utils.isbn.*

import org.codehaus.jackson.map.*
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

interface RestAPI extends API {
    String getPath()
}

@Log
abstract class BasicWhelkAPI extends Restlet implements RestAPI {
    Whelk whelk

    def pathEnd = ""
    def varPath = false
    boolean enabled = true

    int order = 0

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    void setWhelk(Whelk w) {
        this.whelk = w
    }
    @Override
    void init(String w) {}

    String getPath() {
        def elems = [(this.whelk instanceof HttpWhelk ? this.whelk.contentRoot : this.whelk.id), getPathEnd()]
        return "/" + elems.findAll { it.length() > 0 }.join("/")
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

    abstract void doHandle(Request request, Response response)

    void handle(Request request, Response response) {
        long startTime = System.currentTimeMillis()
        doHandle(request, response)
        def headers = request.attributes.get("org.restlet.http.headers")
        def remote_ip = headers.find { it.name.equalsIgnoreCase("X-Forwarded-For") }?.value ?: request.clientInfo.address
        log.info("Handled ${request.method.toString()} request for ${this.id} from $remote_ip in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
    }
}

@Log
class DiscoveryAPI extends BasicWhelkAPI {

    ObjectMapper mapper = new ObjectMapper()

    String description = "Discovery API"
    String pathEnd = "discovery"
    String id = "DiscoveryAPI"

    DiscoveryAPI(Whelk w) {
        this.whelk = w
    }

    @Override
    void doHandle(Request request, Response response) {
        def info = [:]
        info["whelk"] = whelk.id
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
    String id = "RootRoute"
    def pathEnd = ""

    RootRouteRestlet(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    String getPath() {
        if (this.whelk.contentRoot == "") {
            return "/"
        } else return "/" + this.whelk.contentRoot + "/"
    }

    @Override
    void doHandle(Request request, Response response) {
        def discoveryAPI
        def documentAPI
        if (request.method == Method.GET) {
            discoveryAPI = new DiscoveryAPI(this.whelk)
            def uri = new URI(discoveryAPI.path)
            discoveryAPI.handle(request, response)
        } else if (request.method == Method.POST) {
            try {
                def identifier
                Document doc = null
                def headers = request.attributes.get("org.restlet.http.headers")
                log.trace("headers: $headers")
                log.debug("request: $request")
                def link = headers.find { it.name.equals("link") }?.value
                doc = this.whelk.createDocument(Tools.normalizeString(request.entityAsText), ["contentType":request.entity.mediaType.toString()])
                if (link != null) {
                    log.trace("Adding link $link to document...")
                    doc = doc.withLink(link)
                }
                identifier = this.whelk.add(doc)
                response.setEntity(doc.dataAsString, LibrisXLMediaType.getMainMediaType(doc.contentType))
                response.entity.setTag(new Tag(doc.timestamp as String, false))
                log.debug("Saved document $identifier")
                response.setStatus(Status.REDIRECTION_SEE_OTHER, "Thank you! Document ingested with id ${identifier}")
                log.info("Redirecting with location ref " + request.getRootRef().toString() + identifier)
                response.setLocationRef(request.getRootRef().toString() + "${identifier}")
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        } else if (request.method == Method.PUT) {
            documentAPI = new DocumentRestlet(this.whelk)
            if (request.attributes?.get("identifier") != null) {
                documentAPI.handle(request, response)
            } else {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST)
            }
        }
    }
}

@Log
class DocumentRestlet extends BasicWhelkAPI {

    def pathEnd = "{identifier}"
    def varPath = true

    String description = "A GET request with identifier loads a document. A PUT request stores a document. A DELETE request deletes a document."
    String id = "DocumentAPI"

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

    void doHandle(Request request, Response response) {
        log.debug("reqattr path: " + request.attributes["identifier"])
        String path = path.replaceAll(_escape_regex(pathEnd), request.attributes["identifier"])
        log.debug "Path: $path"
        def mode = DisplayMode.DOCUMENT
        (path, mode) = determineDisplayMode(path)
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
                        response.setEntity(d.dataAsString, LibrisXLMediaType.getMainMediaType(d.contentType))
                    }
                    response.entity.setTag(new Tag(d.timestamp as String, false))
                } else {
                    log.debug("Failed to find a document with URI $path")
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND)
                }
            } catch (WhelkRuntimeException wrte) {
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
            }
        } else if (request.method == Method.POST) {
            if (request.attributes?.get("identifier") != null) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST)
            }
        } else if (request.method == Method.PUT) {
            try {
                if (path == "/") {
                    throw new WhelkRuntimeException("PUT requires a proper URI.")
                }
                def identifier
                Document doc = null
                Document rawdoc = null
                def headers = request.attributes.get("org.restlet.http.headers")
                log.trace("headers: $headers")
                def link = headers.find { it.name.equals("link") }?.value
                doc = this.whelk.createDocument(request.entityAsText, ["identifier":path,"contentType":request.entity.mediaType.toString()])
                rawdoc = this.whelk.createDocument(request.entityAsText, ["identifier":path,"contentType":request.entity.mediaType.toString()], false)
                if (rawdoc?.contentType == doc?.contentType) {
                    // No need to doublestore document.
                    rawdoc = null
                }
                log.debug("Created document with id ${doc.identifier}, ct: ${doc.contentType} ($path)")
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
                    if (rawdoc) {
                        try {
                            log.debug("Adding raw version of ${rawdoc.identifier}")
                            this.whelk.add(rawdoc)
                        } catch (WhelkAddException wae) {
                            log.debug("Failed to add raw version: ${wae.message}")
                        }
                    }
                    // If no dedicated storage for the raw type is available,
                    // make sure the converted version overwrites the raw.
                    try {
                        identifier = this.whelk.add(doc)
                        response.setStatus(Status.REDIRECTION_SEE_OTHER, "Thank you! Document ingested with id ${identifier}")
                        response.setLocationRef(request.getOriginalRef().toString())
                    } catch (WhelkAddException wae) {
                        log.warn("Whelk failed to store document: ${wae.message}")
                        response.setStatus(Status.CLIENT_ERROR_NOT_ACCEPTABLE, wae.message)
                    }
                }
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, wre.message)
            }
        }
        else if (request.method == Method.DELETE) {
            try {
                whelk.remove(new URI(path))
                response.setStatus(Status.SUCCESS_NO_CONTENT)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(Status.SERVER_ERROR_INTERNAL, wre.message)
            }
        }
    }
}

@Log
class SparqlRestlet extends BasicWhelkAPI {
    def pathEnd = "_sparql"
    def varPath = false
    String id = "SparqlAPI"

    String description = "Provides sparql endpoint to the underlying tripple store."

    @Override
    void doHandle(Request request, Response response) {
        ClientResource resource = new ClientResource(this.whelk.graphStores[0].queryURI)
        response.setEntity(resource.handleOutbound(request))
    }
}

@Log
class SearchRestlet extends BasicWhelkAPI {

    def pathEnd = "_find"
    //String path = "/{identifier}/_find"
    def varPath = false
    def defaultQueryParams = [:]
    String id = "SearchAPI"

    String description = "Generic search API, accepts both GET and POST requests. Accepts parameters compatible with the Query object. (Simple usage: ?q=searchterm)"


    SearchRestlet(Map queryParams) {
        this.defaultQueryParams = queryParams
    }

    @Override
    void doHandle(Request request, Response response) {
        log.debug "SearchRestlet on ${whelk.id} with path $path"

        def reqMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        try {
            this.defaultQueryParams.each { key, value ->
                if (!reqMap[key]) {
                    reqMap[key] = value
                }
            }
            log.debug("reqMap: $reqMap")
            def query = new ElasticQuery(reqMap)
            def callback = reqMap.get("callback")
            def results = this.whelk.search(query)
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
class FieldSearchRestlet extends BasicWhelkAPI {
    def pathEnd = "_fieldsearch/{indexType}"
    String id = "ESFieldSearch"
    def varPath = false
    String description = "Query API for field searches. For example q=about.instanceOf.creator.controlledLabel:Strindberg, August"

    void doHandle(Request request, Response response) {
        def reqMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def callback = reqMap.get("callback")
        def q = reqMap["q"]
        def remainingMap
        def indexType = request.attributes.get("indexType")

        if (!indexType) {
            response.setEntity("Missing \"indexType\" in url", MediaType.PLAIN_TEXT_UTF_8)
        }

        log.debug("index type $indexType")

        def query = new ElasticQuery(q).withType(indexType)
        def results = this.whelk.search(query)

        def jsonResult =
            (callback ? callback + "(" : "") +
                    results.toJson() +
                    (callback ? ");" : "")
        response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
    }
}

@Log
class KitinSearchRestlet2 extends BasicWhelkAPI {
    def pathEnd = "kitin/{indexType}/_search"
    def varPath = false
    String id = "KitinSearch"

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

    def keys = [
                "bib" : [
                    "about.@type",
                    "about.title",
                    "about.responsibilityStatement",
                    "about.publication",
                    "about.identifier",
                    "about.instanceOf.creator",
                    "about.instanceOf.contributorList"
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

    void doHandle(Request request, Response response) {}

    @Override
    void handle(Request request, Response response) {
        long startTime = System.currentTimeMillis()
        def reqMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def q, results
        if (!reqMap["boost"]) {
            reqMap["boost"] = defaultBoost
        }
        try {
            q = new ElasticQuery(reqMap)
            q.indexType = request.attributes["indexType"]
            if (reqMap["f"]) {
                q.query = (q.query + " " + reqMap["f"]).trim()
            }
            def callback = reqMap.get("callback")
            if (q) {
                q.addFacet("about.@type")
                log.debug("Query $q.query Fields: $q.fields Facets: $q.facets")
                //q.addScriptFieldFacet("about.dateOfPublication")
                //q.addFacet("about.dateOfPublication")
                /*
                q.addFacet("typeOfRecord")
                q.addFacet("bibLevel")
                 */
                //q = addQueryFacets(q)
                q = expandQuery(q)
                results = this.whelk.search(q)
                def keyList
                if (keys.containsKey(q.indexType)) {
                    keyList = keys[q.indexType]
                }
                if (reqMap["_keys"]) {
                    keyList = reqMap["_keys"].split(":") as List
                }
                def extractedResults
                if (keyList) {
                    extractedResults = results.toJson(keyList)
                } else {
                    extractedResults = results.toJson()
                }

                def jsonResult =
                (callback ? callback + "(" : "") +
                    extractedResults +
                (callback ? ");" : "")

                response.setEntity(jsonResult,  MediaType.APPLICATION_JSON)
            } else {
                response.setEntity("Missing q parameter",  MediaType.TEXT_PLAIN)
            }
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        } finally {
            def headers = request.attributes.get("org.restlet.http.headers")
            def remote_ip = headers.find { it.name.equalsIgnoreCase("X-Forwarded-For") }?.value ?: request.clientInfo.address
            log.info("Query [" + q?.query + "] completed for $remote_ip in " + (System.currentTimeMillis() - startTime) + " milliseconds and resulted in " + results?.numberOfHits + " hits.")
        }
    }
}

@Log
class ISXNTool extends BasicWhelkAPI {
    def pathEnd = "_isxntool"
    String id = "ISXNTool"
    String description = "Formats data (ISBN-numbers) according to international presention rules."
    Whelk dataWhelk
    ObjectMapper mapper = new ObjectMapper()

    ISXNTool(Whelk dw) {
        this.dataWhelk = dw
    }

    void doHandle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String isbnString = querymap.get("isbn")
        if (isbnString) {
            handleIsbn(isbnString, querymap, request, response)
        } else {
            response.setEntity('{"error":"No valid parameter found."}', MediaType.APPLICATION_JSON)
        }
    }

    void handleIsbn(String isbnString, Map querymap, Request request, Response response) {
        String providedIsbn = isbnString
        isbnString = isbnString.replaceAll(/[^\dxX]/, "")
        def isbnmap = [:]
        isbnmap["provided"] = providedIsbn
        Isbn isbn = null
        try {
            isbn = IsbnParser.parse(isbnString)
        }  catch (se.kb.libris.utils.isbn.IsbnException iobe) {
            log.debug("Validation of isbn $isbnString failed: wrong length")
        }
        if (isbn) {
            log.debug("isbnString: $isbnString")
            String formattedIsbn = isbn.toString(true)
            String properIsbn = isbn.toString()
            boolean checkExists = (querymap.get("check", "false") == "true")
            boolean ignoreValid = (querymap.get("ignoreValidation", "false") == "true")
            boolean isValid = validISBN(isbnString)
            isbnmap["valid"] = isValid
            if (ignoreValid || isValid) {
                isbnmap["formatted"] = formattedIsbn
                isbnmap["proper"] = properIsbn
            }
            if (checkExists) {
                def results = dataWhelk.search(new Query(isbn.toString()).addField("about.isbn"))
                isbnmap["exists"] = (results.numberOfHits > 0)
            }
        } else {
            isbnmap["valid"] = false
            isbnmap["error"] = new String("Failed to parse $providedIsbn as ISBN")
        }
        response.setEntity(mapper.writeValueAsString(["isbn":isbnmap]), MediaType.APPLICATION_JSON)
    }

    boolean validISBN(String isbn) {
        boolean valid = false
        int n = 0
        try {
            // calculate sum
            if (isbn.length() == 10) {
                for (int i=0;i<isbn.length()-1;i++) {
                    n += Character.getNumericValue(isbn.charAt(i))*Isbn.weights[i];
                }
                n %= 11;
                valid = (isbn.charAt(9) == (n == 10 ? 'X' : (""+n).charAt(0)))
                log.debug("isbn10 check digit: $n ($valid)")
            } else if (isbn.length() == 13) {
                for (int i=0;i<isbn.length()-1;i++)
                n += Character.getNumericValue(isbn.charAt(i))*Isbn.weights13[i];
                n = (10 - (n % 10)) % 10;
                valid = (isbn.charAt(12) == (""+n).charAt(0))
                log.debug("isbn13 check digit: $n ($valid)")
            }
        } catch (Exception e) {
            return valid
        }
        //return (n==10)? 'X' : (char)(n + '0');
        return valid
    }
}

@Log
class CompleteExpander extends BasicWhelkAPI {
    def pathEnd = "_expand"
    String id = "CompleteExpander"
    String description = "Provides useful information about person autorities."

    void doHandle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String url = querymap.get("id")
        String name = querymap.get("name")
        if (url) {
            def r = whelk.search(new ElasticQuery(url).addField("_id"))
        } else if (name) {
            def r = whelk.search(new ElasticQuery("about.instanceOf.authorList.authorizedAccessPoint.untouched", name).withType("bib"))
            //def r = whelk.search(new ElasticQuery("internalRemark.untouched", name).withType("bib"))
            response.setEntity(r.toJson(), MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"error":"Parameter \"id\" is missing."}', MediaType.APPLICATION_JSON)
        }
    }
}

@Log
class AutoComplete extends BasicWhelkAPI {

    def pathEnd = "_complete"

    def namePrefixes = []
    def extraInfo = []
    def sortby = []

    def queryType = "q"

    String types
    String description = "Search API for autocompletion. For person-completion: use pathend _complete with parameter name. For concept-completion: use _subjcomplete pathend with parameter concept."
    String id = "AutoComplete"

    /*
    def void addNamePrefix(String prefix) {
    namePrefixes << prefix
    }
     */

    AutoComplete(Map lists) {
        namePrefixes.addAll(lists.get("queryFields"))
        extraInfo.addAll(lists.get("infoFields"))
        types = lists.get("indexTypes")
        sortby = lists.get("sortby")
        if (lists.get("pathEnd", null)) {
            this.pathEnd = lists.get("pathEnd")
        }
        if (lists.get("queryType", null)) {
            this.queryType = lists.get("queryType")
        }
    }

    String splitName(String name) {
        def np = []
        for (n in name.split(/[\s-_]/)) {
            n = n.replaceAll(/\W$+/, "")
            if (n[-1] != ' ' && n[-1] != '*' && n.length() > 1) {
                np << n+"*"
            } else if (n) {
                np << n
            }

        }
        return np.join(" ")
    }

    @Override
    void doHandle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        log.debug("Handling autocomplete request with queryType: $queryType...")
        String queryStr = querymap.get(queryType, null)
        log.debug("QueryStr: $queryStr")
        def callback = querymap.get("callback")
        if (queryStr) {
            queryStr = splitName(queryStr)
            log.debug("Query fields added to highlight: $namePrefixes")
            def query = new ElasticQuery(queryStr)
            query.highlights = namePrefixes
            query.sorting = sortby
            query.fields = namePrefixes
            query.addBoost(namePrefixes[0], 10)
            query.indexType = types

            def results = this.whelk.search(query)

            def jsonResult = 
            (callback ? callback + "(" : "") +
                new SuggestResultsConverter(results, [namePrefixes[0]], extraInfo).toJson() +
            (callback ? ");" : "")

            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
        } else {
            response.setEntity('{"error":"QueryParameter \"name\", \"concept\" or \"q\" is missing."}', MediaType.APPLICATION_JSON)
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
            if (it.identifier.toString() =~ /\/auth\/\d+/) {
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
class ResourceListRestlet extends BasicWhelkAPI {
    def pathEnd = "_resourcelist"

    String description = "Query API for language and country codes."
    String id = "ResourceListAPI"

    def codeFiles = [
        "lang": "langcodes.json",
        "country": "countrycodes.json",
        "nationality": "nationalitycodes.json",
        "relator": "relatorcodes.json",
        "conceptscheme": "conceptschemes.json",
        "typedef": "typedefs.json"
    ]
    def lang
    def country
    def nationality
    def relator
    def conceptscheme
    def typedef
    def mapper

    ResourceListRestlet() {
        mapper = new ObjectMapper()
        codeFiles.each { key, fileName ->
            log.debug("Load codes $key")
            this[(key)] = loadCodes(key)
        }
    }

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

    void doHandle(Request request, Response response) {
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

/*@Log
class TemplateRestlet extends BasicWhelkAPI {
def pathEnd = "_template"

String description = "API for templates."
String id = "TemplateAPI"

String templateDir =
}*/

@Log
class MarcMapRestlet extends BasicWhelkAPI {
    def pathEnd = "_marcmap"

    String description = "API for marcmap."
    String id = "MarcMapAPI"

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

    void doHandle(Request request, Response response) {
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
    String id = "MetadataSearchAPI"

    void doHandle(Request request, Response response) {
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
            queryObj = new ElasticQuery("${link} AND ${tag}")
        } else if (link) {
            queryObj = new ElasticQuery(link)
        } else if (tag) {
            queryObj = new ElasticQuery(tag)
        }

        if (queryObj) {
            queryObj.indexType = "Indexed:Metadata"
            results = this.whelk.search(queryObj)

            results.hits.each {
                def identifier = it.identifier.toString()
                def d = whelk.get(new URI(identifier))
                if (d) {
                    records << d
                }
            }

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
            response.setEntity('{"Error":"Use parameter \"link=<uri>\". and/or \"tag=<sigel>\"}', MediaType.APPLICATION_JSON)
        }
    }

}

@Log
class RemoteSearchRestlet extends BasicWhelkAPI {
    def pathEnd = "_remotesearch"
    def mapper
    
    String description = "Query API for remote search"
    String id = "RemoteSearchAPI"

    def remoteTestURL = "http://mproxy.libris.kb.se:8000/LC?version=1.1&operation=searchRetrieve&maximumRecords=10"

    void doHandle(Request request, Response response) {
        def queryMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def query = queryMap.get("q", null)
        def xmlRecords
        if (query) {
            def url = new URL("${remoteTestURL}&query=" + query)
            try {
                xmlRecords = new XmlSlurper().parseText(url.text).declareNamespace(zs:"http://www.loc.gov/zing/srw/", tag0:"http://www.loc.gov/MARC21/slim")
            } catch (org.xml.sax.SAXParseException spe) {
                log.error("Failed to parse XML: ${url.text}")
                throw spe
            }
            def docs = convertXMLRecords(xmlRecords)
            /*mapper = new ObjectMapper()
            def jsonResult
            for (doc in docs) {
                docMap = mapper.readValue(doc, Map)
                def res = mapper.writeValueAsString(docMap)
                log.debug("RES ${res}")
                jsonResult += mapper.writeValueAsString(docMap)
            }
            log.debug("RESULT: " + jsonResult)
            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
            */
            //XML response for now...
            def responseXMLString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>\n"
            for (doc in docs) {
               responseXMLString += doc
            }
            response.setEntity(responseXMLString + "\n</records>", MediaType.APPLICATION_XML)
        } else {
            response.setEntity(/{"Error": "Use parameter \"q\"}/, MediaType.APPLICATION_JSON)
        }
    }

    List<Document> convertXMLRecords(def xmlRecords) {
        def documents = []
        xmlRecords?.'zs:records'?.each { rec ->
            rec.each { it ->
                def marcXmlRecord = it?.'zs:record'?.'zs:recordData'?.'tag0:record'
                def marcXmlRecordString = createString(marcXmlRecord)
                if (marcXmlRecordString) {
                    log.debug("MARCXMLRECORDSTRING: ${marcXmlRecordString}")
                    documents << marcXmlRecordString
                    /*MarcRecord record = MarcXmlRecordReader.fromXml(marcXmlRecordString)
                    String id = record.getControlfields("001").get(0).getData()
                    String jsonRec = MarcJSONConverter.toJSONString(record)
                    try {
                        documents << whelk.createDocument(jsonRec.getBytes("UTF-8"), ["identifier":new URI("/remote/"+id),"contentType":"application/x-marc-json" ])
                    } catch (Exception e) {
                        log.error("Failed! (${e.message}) for :\n$mdrecord")
                    } */
                }
            }
        }
        return documents
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind {
            out << root
        }
    }
}

