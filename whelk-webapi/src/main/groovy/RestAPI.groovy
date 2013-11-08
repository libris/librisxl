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
        //GET redirects to DiscoveryAPI
        if (request.method == Method.GET) {
            discoveryAPI = new DiscoveryAPI(this.whelk)
            def uri = new URI(discoveryAPI.path)
            discoveryAPI.handle(request, response)
          //POST saves new document
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
                log.debug("Redirecting with location ref " + request.getRootRef().toString() + identifier)
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
                        response.setEntity(d.metadataAsJson, MediaType.APPLICATION_JSON)
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
                doc = this.whelk.createDocument(request.entityAsText, ["identifier":path,"contentType":request.entity.mediaType.toString(),"dataset":path.split("/")[1]])
                rawdoc = this.whelk.createDocument(request.entityAsText, ["identifier":path,"contentType":request.entity.mediaType.toString(),"dataset":path.split("/")[1]], null, false)
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
                        response.setStatus(Status.CLIENT_ERROR_UNSUPPORTED_MEDIA_TYPE, wae.message)
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

@Deprecated
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
class SearchRestlet extends BasicWhelkAPI {
    def pathEnd = "{indexType}/_search"
    def varPath = false
    String id = "Search"
    String description = "Generic search query API. User parameters \"q\" for querystring, and optionally \"facets\" and \"boost\"."
    def config

    SearchRestlet(indexTypeConfig) {
        this.config = indexTypeConfig
    }

    @Override
    void doHandle(Request request, Response response) {
        long startTime = System.currentTimeMillis()
        def queryMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def indexType, results
        try {
            indexType = request.attributes.indexType
        } catch(Exception e) {
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, MediaType.TEXT_PLAIN)
        }
        log.debug("Handling search request with indextype $indexType")
        def indexConfig = config.indexTypes[indexType]
        def boost = queryMap.boost?.split() ?: indexConfig?.defaultBoost?.split(",")
        def facets = queryMap.facets?.split() ?: indexConfig?.queryFacets?.split(",")
        /*if (indexConfig?.containsKey("queryStringTokenizerMethod") && indexConfig.queryStringTokenizerMethod.size() > 0) {
            Method tokenizerMethod = queryStringTokenizer = this.class.getDeclaredMethod(indexConfig.queryStringTokenizerMethod)
            queryMap.q = tokenizerMethod?.invoke(this, queryMap.q) ?: queryMap.q
        } */
        def elasticQuery = new ElasticQuery(queryMap)
        if (queryMap.f) {
            elasticQuery.query += " " + queryMap.f
        }
        elasticQuery.indexType = indexType
        if (facets) {
            for (f in facets) {
                elasticQuery.addFacet(f)
            }
        }
        if (boost) {
            for (b in boost) {
                if (b.size() > 0) {
                    def (k, v) = b.split(":")
                    elasticQuery.addBoost(k, Long.parseLong(v))
                }
            }
        }
        def fields = indexConfig?.get("queryFields")
        if (fields && fields.size() > 0) {
            elasticQuery.fields = fields
        }
        elasticQuery.highlights = indexConfig?.get("queryFields")
        elasticQuery.sorting = indexConfig?.get("sortby")
        log.debug("Query $elasticQuery.query Fields: ${elasticQuery?.get("fields")} Facets: ${elasticQuery?.get("facets")}")
        try {
            def callback = queryMap.get("callback")
            results = this.whelk.search(elasticQuery)
            def keyList = queryMap.resultFields?.split() as List ?: indexConfig?.get("resultFields")
            def extractedResults = keyList ? results.toJson(keyList) : results.toJson()
            def jsonResult =
                    (callback ? callback + "(" : "") +
                        extractedResults +
                    (callback ? ");" : "")

            response.setEntity(jsonResult, MediaType.APPLICATION_JSON)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        } finally {
            def headers = request.attributes.get("org.restlet.http.headers")
            def remote_ip = headers.find { it.name.equalsIgnoreCase("X-Forwarded-For") }?.value ?: request.clientInfo.address
            log.info("Query [" + elasticQuery?.query + "] completed for $remote_ip in " + (System.currentTimeMillis() - startTime) + " milliseconds and resulted in " + results?.numberOfHits + " hits.")
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
    String description = "Provides useful information about authorities."

    //TODO: in param: auth-id, search for matches in type (=bib)

    void doHandle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String type = querymap.get("type")
        String url = querymap.get("id")
        def result
        if (type && url) {
            result = whelk.search(new ElasticQuery(url).addField("about.instanceOf.creator.@id", "about.instanceOf.contributorList.@id"))
        } else {
            response.setEntity('{"error":"Parameter \"type\" and/or \"id\" is missing."}', MediaType.APPLICATION_JSON)
        }
        response.setEntity(result.toJson(), MediaType.APPLICATION_JSON)
    }
}

@Deprecated
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
            n = n.replaceAll(/\W$+^\*/, "")
            if (n.length() > 1 && n[-1] != ' ' && n[-1] != '*') {
                np << n+"*"
            } else if (n) {
                np << n
            }
        }
        log.debug(np.join(" "))
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
        }
        out["hits"] = results.numberOfHits
        out["list"] = list
        return mapper.writeValueAsString(out)
    }

    @Deprecated
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

    @Deprecated
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


@Deprecated
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

@Log
class DefinitionDataRestlet extends Directory implements RestAPI {
    Whelk whelk
    String id = "DefinitionDataAPI"
    String path = "/def"
    boolean enabled = true
    def varPath = false
    void init(String w) {}

    DefinitionDataRestlet(String fileRoot) {
        super(null, toAbsoluteFileUri(fileRoot) as String)
    }

    static URI toAbsoluteFileUri(String path) {
        if (path.indexOf(":") == -1) {
            path = path.startsWith("/")? path :
                    System.getProperty("user.dir") + "/" + path
            path = "file://" + path
        }
        if (!path.endsWith("/")) {
            path += "/"
        }
        return new URI(path)
    }

}


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


@Deprecated
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

