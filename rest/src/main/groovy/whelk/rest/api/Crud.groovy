package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import org.apache.http.entity.ContentType
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.PicoContainer

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary

import whelk.Document
import whelk.IdType
import whelk.JsonLd
import whelk.Whelk
import whelk.IdGenerator
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.FormatConverter
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.exception.InvalidQueryException
import whelk.exception.ModelValidationException
import whelk.exception.StorageCreateFailedException
import whelk.exception.WhelkAddException
import whelk.exception.WhelkRuntimeException
import whelk.exception.WhelkStorageException
import whelk.rest.api.CrudUtils
import whelk.rest.api.MimeTypes
import whelk.rest.api.SearchUtils
import whelk.rest.security.AccessControl
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

import javax.activation.MimetypesFileTypeMap
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.lang.management.ManagementFactory

import static HttpTools.sendResponse
import static whelk.rest.api.HttpTools.getMajorContentType

/**
 * Handles all GET/PUT/POST/DELETE requests against the backend.
 */
@Log
class Crud extends HttpServlet {

    final static String SAMEAS_NAMESPACE = "http://www.w3.org/2002/07/owl#sameAs"
    final static String DOCBASE_URI = "http://libris.kb.se/" // TODO: encapsulate and configure (LXL-260)

    static final Counter requests = Counter.build()
        .name("api_requests_total").help("Total requests to API.")
        .labelNames("method").register()

    static final Counter failedRequests = Counter.build()
        .name("api_failed_requests_total").help("Total failed requests to API.")
        .labelNames("method", "resource", "status").register()

    static final Gauge ongoingRequests = Gauge.build()
        .name("api_ongoing_requests_total").help("Total ongoing API requests.")
        .labelNames("method").register()

    static final Summary requestsLatency = Summary.build()
        .name("api_requests_latency_seconds")
        .help("API request latency in seconds.")
        .labelNames("method").register()

    enum FormattingType {
        FRAMED, EMBELLISHED, FRAMED_AND_EMBELLISHED, RAW
    }

    static final JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();

    MimetypesFileTypeMap mimeTypes = new MimetypesFileTypeMap()

    final static Map contextHeaders = [
            "bib": "/sys/context/lib.jsonld",
            "auth": "/sys/context/lib.jsonld",
            "hold": "/sys/context/lib.jsonld"
    ]
    Whelk whelk

    Map vocabData
    Map displayData
    JsonLd jsonld

    SearchUtils search
    PicoContainer pico
    static final ObjectMapper mapper = new ObjectMapper()
    AccessControl accessControl = new AccessControl()

    Crud() {
        super()
        log.info("Setting up httpwhelk.")

        Properties props = PropertyLoader.loadProperties("secret")

        // Get a properties pico container, pre-wired with components according to components.properties
        pico = Whelk.getPreparedComponentsContainer(props)

        pico.addComponent(JsonLD2MarcConverter.class)
        pico.addComponent(JsonLD2MarcXMLConverter.class)
        pico.start()
    }

    @Override
    void init() {
        whelk = pico.getComponent(Whelk.class)
        whelk.loadCoreData()
        displayData = whelk.displayData
        vocabData = whelk.vocabData
        jsonld = whelk.jsonld
        search = new SearchUtils(whelk, displayData, vocabData)
    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response,
                     String dataset) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String callback = queryParameters.remove("callback")

        try {
            Map results = search.doSearch(queryParameters, dataset, jsonld)
            def jsonResult = mapper.writeValueAsString(results)
            sendResponse(response, jsonResult, "application/json")
        } catch (WhelkRuntimeException wse) {
            log.error("Attempted elastic query, but whelk has no " +
                    "elastic component configured.")
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_NOT_IMPLEMENTED.toString()).inc()
            response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED,
                    "Attempted to use elastic for query, but " +
                            "no elastic component is configured.")
            return
        } catch (InvalidQueryException iqe) {
            log.error("Invalid query: ${queryParameters}")
            failedRequests("GET", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid query, please check the documentation.")
            return
        }
    }

    void displayInfo(response) {
        def info = [:]
        info["system"] = "LIBRISXL"
        info["format"] = "linked-data-api"
        //info["collections"] = whelk.storage.loadCollections()
        sendResponse(response, mapper.writeValueAsString(info), "application/json")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("GET").inc()
        ongoingRequests.labels("GET").inc()
        Summary.Timer requestTimer = requestsLatency.labels("GET").startTimer()
        log.info("Handling GET request for ${request.pathInfo}")
        try {
            doGet2(request, response)
        } finally {
            ongoingRequests.labels("GET").dec()
            requestTimer.observeDuration()
            log.info("Sending GET response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doGet2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            displayInfo(response)
            return
        }

        if (request.pathInfo == "/find" || request.pathInfo == "/find.json") {
            String collection = request.getParameter("collection")
            handleQuery(request, response, collection)
            return
        }

        def marcframePath = "/sys/marcframe.json"
        if (request.pathInfo == marcframePath) {
            def responseBody = getClass().classLoader.getResourceAsStream("ext/marcframe.json").getText("utf-8")
            sendGetResponse(request, response, responseBody, "1970/1/1", marcframePath, "application/json")
            return
        }

        def forcedsetPath = "/sys/forcedsetterms.json"
        if (request.pathInfo == forcedsetPath) {
            String responseBody = mapper.writeValueAsString(jsonld.forcedSetTerms)
            sendGetResponse(request, response, responseBody, "1970/1/1", forcedsetPath, "application/json")
            return
        }

        try {
            def path = getRequestPath(request)

            handleGetRequest(request, response, path)
        } catch (UnsupportedContentTypeException ucte) {
            failedRequests.labels("GET", request.getRequestURI(),
                    response.SC_NOT_ACCEPTABLE.toString()).inc()
            response.sendError(response.SC_NOT_ACCEPTABLE, ucte.message)
        } catch (WhelkRuntimeException wrte) {
            failedRequests.labels("GET", request.getRequestURI(),
                    response.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            response.sendError(response.SC_INTERNAL_SERVER_ERROR, wrte.message)
        }
    }

    void handleGetRequest(HttpServletRequest request,
                          HttpServletResponse response,
                          String path) {
        String id = getIdFromPath(path)
        String version = request.getParameter("version")

        // TODO: return already loaded displayData and vocabData (cached on modified)? (LXL-260)
        Tuple2<Document, String> docAndLocation = getDocumentFromStorage(id, version)
        Document doc = docAndLocation.first
        String loc = docAndLocation.second

        String ifNoneMatch = request.getHeader("If-None-Match")
        if (ifNoneMatch != null && doc != null && ifNoneMatch == doc.getModified()) {
            response.setHeader("ETag", doc.getModified())
            response.setHeader("Server-Start-Time", "" + ManagementFactory.getRuntimeMXBean().getStartTime())
            response.sendError(HttpServletResponse.SC_NOT_MODIFIED,
                    "Document has not been modified.")
            return
        }

        if (!doc && !loc) {
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_NOT_FOUND.toString()).inc()
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    "Document not found.")
            return
        } else if (!doc && loc) {
            sendRedirect(request, response, loc)
            return
        } else if (doc && doc.deleted) {
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_GONE.toString()).inc()
            response.sendError(HttpServletResponse.SC_GONE,
                    "Document has been deleted.")
            return
        } else {
            String contentType = CrudUtils.getBestContentType(request)
            def responseBody = getFormattedResponseBody(doc, path, contentType)
            String modified = doc.getModified()
            response = maybeAddProposal25Headers(response, loc)
            sendGetResponse(request, response, responseBody, modified,
                    path, contentType)
            return
        }
    }

    private Object getFormattedResponseBody(Document doc, String path,
                                            String contentType) {
        FormattingType format = getFormattingType(path, contentType)
        log.debug("Formatting document ${doc.getCompleteId()} with format " +
                "${format} and content type ${contentType}")
        def result
        switch (format) {
            case FormattingType.RAW:
                result = doc.data
                break
            case FormattingType.EMBELLISHED:
                doc = getEmbellishedDocument(doc)
                result = doc.data
                break
            case FormattingType.FRAMED:
                result = JsonLd.frame(doc.getCompleteId(), doc.data)
                break
            case FormattingType.FRAMED_AND_EMBELLISHED:
                doc = getEmbellishedDocument(doc)
                result = JsonLd.frame(doc.getCompleteId(), doc.data)
                break
            default:
                throw new WhelkRuntimeException("Invalid formatting type: ${format}")
        }

        return formatResponseBody(result, contentType)
    }

    private Document getEmbellishedDocument(Document doc) {
        List externalRefs = doc.getExternalRefs()
        List convertedExternalLinks = convertExternalLinks(externalRefs)
        Map referencedData = getReferencedData(convertedExternalLinks)
        boolean filterOutNonChipTerms = false
        doc.embellish(referencedData, jsonld, filterOutNonChipTerms)

        return doc
    }

    /**
     * Format response body based on Content-Type.
     *
     */
    Object formatResponseBody(Object responseBody, String contentType) {
        if (contentType == MimeTypes.JSONLD) {
            return responseBody
        } else if (contentType == MimeTypes.JSON) {
            return responseBody
        } else if (contentType == MimeTypes.RDF) {
            // FIXME convert to RDF-XML
            throw new UnsupportedContentTypeException("Not implemented.")
        } else if (contentType == MimeTypes.TURTLE) {
            // FIXME convert to Turtle
            throw new UnsupportedContentTypeException("Not implemented.")
        } else {
            throw new UnsupportedContentTypeException("Not implemented.")
        }
    }

    private List convertExternalLinks(List refs) {
        return JsonLd.expandLinks(refs, this.displayData[JsonLd.CONTEXT_KEY])
    }

    /**
     * Given a resource path, return the ID part or null.
     *
     * E.g.:
     * getIdFromPath("/foo") -> "foo"
     * getIdFromPath("/foo/data.jsonld") -> "foo"
     * getIdFromPath("/") -> null
     *
     */
    static String getIdFromPath(String path) {
        def pattern = getPathRegex()
        def matcher = path =~ pattern
        if (matcher.matches()) {
            return matcher[0][1]
        } else {
            return null
        }
    }

    /**
     * Return request URI
     *
     */
    private String getRequestPath(HttpServletRequest request) {
        String path = request.getRequestURI()
        // Tomcat incorrectly strips away double slashes from the pathinfo.
        // Compensate here.
        if (path ==~ "/http:/[^/].+") {
            path = path.replace("http:/", "http://")
        } else if (path ==~ "/https:/[^/].+") {
            path = path.replace("https:/", "https://")
        }

        return path
    }

    /**
     * Given a resource path and a content type, return the formatting
     * type for that resource.
     *
     * FormattingType.EMBELLISHED is the default return value.
     *
     */
    static FormattingType getFormattingType(String path, String contentType) {
        log.debug("Getting formatting type for path ${path}")
        int viewGroup = 3
        int fileEndingGroup = 5
        def pattern = getPathRegex()
        def matcher = path =~ pattern
        if (matcher.matches()) {
            String view = matcher.group(viewGroup)
            String fileEnding = matcher.group(fileEndingGroup)
            return getFormattingTypeForView(view, fileEnding, contentType)
        } else {
            return FormattingType.EMBELLISHED
        }
    }

    private static FormattingType getFormattingTypeForView(String view,
                                                           String fileEnding,
                                                           String contentType) {
        FormattingType result

        if (!view) {
            result = getFormattingTypeForFancyView(contentType)
        } else if (view == 'data') {
            switch (fileEnding) {
                case 'jsonld':
                    result = FormattingType.RAW
                    break
                case 'json':
                    result = FormattingType.FRAMED
                    break
                case null:
                    result = getFormattingTypeForSimpleView(contentType)
                    break
                default:
                    // TODO support more file types
                    throw new WhelkRuntimeException("Bad file ending: " +
                            "${fileEnding}")
            }
        } else if (view == 'data-view') {
            switch (fileEnding) {
                case 'jsonld':
                    result = FormattingType.EMBELLISHED
                    break
                case 'json':
                    result = FormattingType.FRAMED_AND_EMBELLISHED
                    break
                case null:
                    result = getFormattingTypeForFancyView(contentType)
                    break
                default:
                    // TODO support more file types
                    throw new WhelkRuntimeException("Bad file ending: ${fileEnding}")
            }
        } else {
            return FormattingType.EMBELLISHED
        }

        return result
    }

    private static FormattingType getFormattingTypeForFancyView(String contentType) {
        FormattingType result

        switch (contentType) {
            case MimeTypes.JSONLD:
                result = FormattingType.EMBELLISHED
                break
            case MimeTypes.JSON:
                result = FormattingType.FRAMED_AND_EMBELLISHED
                break
            default:
                result = FormattingType.EMBELLISHED
                break
        }

        return result
    }

    private static FormattingType getFormattingTypeForSimpleView(String contentType) {
        FormattingType result

        switch (contentType) {
            case MimeTypes.JSONLD:
                result = FormattingType.RAW
                break
            case MimeTypes.JSON:
                result = FormattingType.FRAMED
                break
            default:
                result = FormattingType.RAW
                break
        }

        return result
    }

    /**
     * Return a regex pattern representation of a CRUD path.
     *
     * Matches /foo, /foo/data, /foo/data.<suffix>.
     *
     */
    static String getPathRegex() {
        return ~/^\/(.+?)(\/(data|data-view)(\.(\w+))?)?$/
    }

    /**
     * Get (document, location) from storage for specified ID and version.
     *
     * If version is null, we look for the latest version.
     * Document and String in the response may be null.
     *
     */
    // TODO Handle version requests (See LXL-460)
    Tuple2<Document, String> getDocumentFromStorage(String id,
                                                    String version = null) {
        Tuple2<Document, String> result = new Tuple2(null, null)

        Document doc = whelk.storage.load(id, version)
        if (doc) {
            return new Tuple2(doc, null)
        }

        // we couldn't find the document directly, so we look it up using the
        // identifiers table instead
        switch (whelk.storage.getIdType(id)) {
            case IdType.RecordMainId:
                doc = whelk.storage.loadDocumentByMainId(id, version)
                if (doc) {
                    result = new Tuple2(doc, null)
                }
                break
            case IdType.ThingMainId:
                doc = whelk.storage.loadDocumentByMainId(id, version)
                if (doc) {
                    String contentLocation = whelk.storage.getRecordId(id)
                    result = new Tuple2(doc, contentLocation)
                }
                break
            case IdType.RecordSameAsId:
            case IdType.ThingSameAsId:
                String location = whelk.storage.getMainId(id)
                if (location) {
                    result = new Tuple2(null, location)
                }
                break
            default:
                // 404
                break
        }

        return result
    }

    private HttpServletResponse maybeAddProposal25Headers(HttpServletResponse response,
                                                          String location) {
        if (location) {
            response.addHeader('Content-Location',
                               getDataURI(location))
            response.addHeader('Document', location)
            response.addHeader('Link', "<${location}>; rel=describedby")
        }
        return response
    }

    private String getDataURI(String location) {
        if (location.endsWith('/')) {
            return location + 'data.jsonld'
        } else {
            return location + '/data.jsonld'
        }
    }

    /**
     * Given a list of IDs, return a map of JSON-LD found in storage.
     *
     * Map is of the form ID -> JSON-LD Map.
     *
     */
    Map getReferencedData(List ids) {
        whelk.bulkLoad(ids).collectEntries { id, doc -> [id, doc.data]  }
    }

    /**
     * Format and send GET response to client.
     *
     * Sets the necessary headers and picks the best Content-Type to use.
     *
     */
    void sendGetResponse(HttpServletRequest request,
                         HttpServletResponse response,
                         Object responseBody, String modified, String path,
                         String contentType) {
        // FIXME remove?
        String ctheader = contextHeaders.get(path.split("/")[1])
        if (ctheader) {
            response.setHeader("Link",
                    "<$ctheader>; " +
                            "rel=\"http://www.w3.org/ns/json-ld#context\"; " +
                            "type=\"application/ld+json\"")
        }

        String etag = modified

        response.setHeader("ETag", etag)
        response.setHeader("Server-Start-Time", "" + ManagementFactory.getRuntimeMXBean().getStartTime())

        if (path in contextHeaders.collect { it.value }) {
            log.debug("request is for context file. " +
                    "Must serve original content-type ($contentType).")
            // FIXME what should happen here?
            // contentType = document.contentType
        }

        sendResponse(response, responseBody, contentType)
    }

    Document convertDocumentToAcceptedMediaType(Document document, String path, String acceptHeader, HttpServletResponse response) {

        List<String> accepting = acceptHeader?.split(",").collect {
            int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
            it.substring(0, last)
        }
        log.debug("Accepting $accepting")

        String extensionContentType = (mimeTypes.getContentType(path) == "application/octet-stream" ? null : mimeTypes.getContentType(path))
        log.debug("mimetype: $extensionContentType")
        if (!document && path ==~ /(.*\.\w+)/) {
            log.debug("Found extension in $path")
            if (!document && extensionContentType) {
                document = whelk.storage.load(path.substring(1, path.lastIndexOf(".")))
            }
            accepting = [extensionContentType]
        }

        if (document && accepting && !accepting.contains("*/*") && !accepting.contains(document.contentType) && !accepting.contains(getMajorContentType(document.contentType))) {
            FormatConverter fc = pico.getComponents(FormatConverter.class).find {
                accepting.contains(it.resultContentType) && it.requiredContentType == document.contentType
            }
            if (fc) {
                log.debug("Found formatconverter for ${fc.resultContentType}")
                document = fc.convert(document)
                if (extensionContentType) {
                    response.setHeader("Content-Location", path)
                }
            } else {
                document = null
                throw new UnsupportedContentTypeException("No supported types found in $accepting")
            }
        }
        return document
    }

    /**
     * Send 302 Found response
     *
     */
    void sendRedirect(HttpServletRequest request,
                      HttpServletResponse response, String location) {
        if (new URI(location).getScheme() == null) {
            def locationRef = request.getScheme() + "://" +
                    request.getServerName() +
                    (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") +
                    request.getContextPath()
            location = locationRef + location
        }
        response.setHeader("Location", location)
        log.debug("Redirecting to document location: ${location}")
        sendResponse(response, new byte[0], null, HttpServletResponse.SC_FOUND)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("POST").inc()
        ongoingRequests.labels("POST").inc()
        Summary.Timer requestTimer = requestsLatency.labels("POST").startTimer()
        log.info("Handling POST request for ${request.pathInfo}")

        try {
            doPost2(request, response)
        } finally {
            ongoingRequests.labels("POST").dec()
            requestTimer.observeDuration()
            log.info("Sending POST response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doPost2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo != "/") {
            log.debug("Invalid POST request URL.")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_METHOD_NOT_ALLOWED.toString()).inc()
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                    "Method not allowed.")
            return
        }

        if (!isSupportedContentType(request)) {
            log.debug("Unsupported Content-Type for POST.")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Content-Type not supported.")
            return
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            log.debug("Empty POST request.")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "No data received")
            return
        }

        if (!JsonLd.isFlat(requestBody)) {
            log.debug("POST body is not flat JSON-LD")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Body is not flat JSON-LD.")
        }

        // FIXME we're assuming Content-Type application/ld+json here
        // should we deny the others?

        Document newDoc = new Document(requestBody)
        newDoc.deepReplaceId(Document.BASE_URI.toString() + IdGenerator.generate())

        String collection = LegacyIntegrationTools.determineLegacyCollection(newDoc, jsonld)
        if ( !(collection in ["auth", "bib", "hold"]) ) {
            log.debug("Could not determine legacy collection")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Body could not be categorized as auth, bib or hold.")
        }

        // verify user permissions
        log.debug("Checking permissions for ${newDoc}")
        try {
            boolean allowed = hasPostPermission(newDoc, request.getAttribute("user"))
            if (!allowed) {
                failedRequests.labels("POST", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                response.sendError(HttpServletResponse.SC_FORBIDDEN,
                        "You are not authorized to perform this " +
                                "operation")
                log.debug("Permission check failed. Denying request.")
                return
            }
        } catch (ModelValidationException mve) {
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    mve.getMessage())
            log.debug("Model validation check failed. Denying request: " + mve.getMessage())
            return
        }

        // try store document
        // return 201 or error
        boolean isUpdate = false
        Document savedDoc = saveDocument(newDoc, request, response,
                                         collection, isUpdate, "POST")
        if (savedDoc != null) {
            sendCreateResponse(response, savedDoc.getURI().toString(),
                               savedDoc.getModified() as String)
        }
    }

    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("PUT").inc()
        ongoingRequests.labels("PUT").inc()
        Summary.Timer requestTimer = requestsLatency.labels("PUT").startTimer()
        log.info("Handling PUT request for ${request.pathInfo}")

        try {
            doPut2(request, response)
        } finally {
            ongoingRequests.labels("PUT").dec()
            requestTimer.observeDuration()
            log.info("Sending PUT response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }

    }

    void doPut2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            log.debug("Invalid PUT request URL.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_METHOD_NOT_ALLOWED.toString()).inc()
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                    "Method not allowed.")
            return
        }

        if (!isSupportedContentType(request)) {
            log.debug("Unsupported Content-Type for PUT.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Content-Type not supported.")
            return
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            log.debug("Empty PUT request.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "No data received")
            return
        }

        String documentId = JsonLd.findIdentifier(requestBody)
        String idFromUrl = getRequestPath(request).substring(1)

        if (!documentId) {
            log.debug("Missing document ID in PUT request.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Missing @id in request.")
            return
        }

        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(idFromUrl)
        Document existingDoc = docAndLoc.first
        String location = docAndLoc.second

        if (!existingDoc && !location) {
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_NOT_FOUND.toString()).inc()
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    "Document not found.")
            return
        } else if (!existingDoc && location) {
            sendRedirect(request, response, location)
            return
        } else  {
            String fullPutId = JsonLd.findFullIdentifier(requestBody)
            if (fullPutId != existingDoc.id) {
                log.debug("Record ID for ${existingDoc.id} changed to " +
                          "${fullPutId} in PUT body")
                failedRequests.labels("PUT", request.getRequestURI(),
                        HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "Record ID was modified")
                return
            }
        }

        Document updatedDoc = new Document(requestBody)
        updatedDoc.setId(documentId)

        log.debug("Checking permissions for ${updatedDoc}")
        try {
            // TODO: 'collection' must also match the collection 'existingDoc'
            // is in.
            boolean allowed = hasPutPermission(updatedDoc, existingDoc,
                    request.getAttribute("user"))
            if (!allowed) {
                failedRequests.labels("PUT", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                response.sendError(HttpServletResponse.SC_FORBIDDEN,
                        "You are not authorized to perform this " +
                                "operation")
                return
            }
        } catch (ModelValidationException mve) {
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    mve.getMessage())
            return
        }

        String collection = LegacyIntegrationTools.determineLegacyCollection(updatedDoc, jsonld)
        if ( !(collection in ["auth", "bib", "hold"]) ) {
            log.debug("Could not determine legacy collection")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Body could not be categorized as auth, bib or hold.")
        }

        boolean isUpdate = true
        Document savedDoc = saveDocument(updatedDoc, request, response,
                                         collection, isUpdate, "PUT")
        if (savedDoc != null) {
            sendUpdateResponse(response, savedDoc.getURI().toString(),
                               savedDoc.getModified() as String)
        }

    }

    boolean isEmptyInput(Map inputData) {
        if (!inputData || inputData.size() == 0) {
            return true
        } else {
            return false
        }
    }

    boolean isSupportedContentType(HttpServletRequest request) {
        ContentType contentType = ContentType.parse(request.getContentType())
        String mimeType = contentType.getMimeType()
        // FIXME add additional content types?
        if (mimeType == "application/ld+json") {
            return true
        } else {
            return false
        }
    }

    Map getRequestBody(HttpServletRequest request) {
        byte[] body = request.getInputStream().getBytes()

        try {
            return mapper.readValue(body, Map)
        } catch (EOFException) {
            return [:]
        }
    }

    private class EtagMissmatchException extends RuntimeException {}

    Document saveDocument(Document doc, HttpServletRequest request,
                          HttpServletResponse response, String collection,
                          boolean isUpdate, String httpMethod) {
        try {
            if (doc) {

                if (isUpdate) {
                    whelk.storeAtomicUpdate(doc.getShortId(), false, "xl", null, collection, false, {
                        Document _doc ->
                            if (_doc.modified as String != request.getHeader("If-Match")) {
                                log.debug("PUT performed on stale document.")

                                throw new EtagMissmatchException()
                            }

                            log.debug("All checks passed.")
                            log.debug("Saving UPDATE of document ("+ doc.getId() +")")

                            // Replace our data with the incoming data.
                            _doc.data = doc.data
                    })
                }
                else {
                    log.debug("Saving NEW document ("+ doc.getId() +")")
                    doc = whelk.createDocument(doc, "xl", null, collection, false)
                }

                log.debug("Saving document (${doc.getShortId()})")
                log.info("Document accepted: created is: ${doc.getCreated()}")

                return doc
            }
        } catch (StorageCreateFailedException scfe) {
            log.warn("Refused document with id ${scfe.duplicateId}")
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_CONFLICT.toString()).inc()
            response.sendError(HttpServletResponse.SC_CONFLICT,
                    scfe.message)
            return null
        } catch (WhelkAddException wae) {
            log.warn("Whelk failed to store document: ${wae.message}")
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE.toString()).inc()
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE,
                    wae.message)
            return null
        } catch (EtagMissmatchException eme) {
            log.warn("Did not store document, because the ETAGs did not match.")
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_PRECONDITION_FAILED.toString()).inc()
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED,
                                        "The resource has been updated by someone " +
                                                "else. Please refetch.")
            return null
        } catch (PostgreSQLComponent.AcquireLockException e) {
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Failed to acquire a necessary lock. Did you submit a holding record without a valid bib link? " + e.message)
            return null
        } catch (PostgreSQLComponent.ConflictingHoldException e) {
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
            return null
        } catch (Exception e) {
            log.error("Operation failed", e)
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    e.message)
            return null
        }
        return null
    }

    void sendCreateResponse(HttpServletResponse response, String locationRef,
                            String etag) {
        sendDocumentSavedResponse(response, locationRef, etag, true)
    }

    void sendUpdateResponse(HttpServletResponse response, String locationRef,
                            String etag) {
        sendDocumentSavedResponse(response, locationRef, etag, false)
    }

    void sendDocumentSavedResponse(HttpServletResponse response,
                                   String locationRef, String etag,
                                   boolean newDocument) {
        log.debug("Setting header Location: $locationRef")

        response.setHeader("Location", locationRef)
        response.setHeader("ETag", etag as String)

        if (newDocument) {
            response.setStatus(HttpServletResponse.SC_CREATED)
        } else {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT)
        }
    }

    boolean hasPostPermission(Document newDoc, Map userInfo) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (isSystemUser(userInfo)) {
                return true
            } else {
                return accessControl.checkDocumentToPost(newDoc, userInfo, jsonld)
            }
        }
        log.info("No user information received, denying request.")
        return false
    }

    boolean hasPutPermission(Document newDoc, Document oldDoc, Map userInfo) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (isSystemUser(userInfo)) {
                return true
            } else {
                return accessControl.checkDocumentToPut(newDoc, oldDoc, userInfo, jsonld)
            }
        }
        log.info("No user information received, denying request.")
        return false
    }

    boolean hasDeletePermission(Document oldDoc, Map userInfo) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (isSystemUser(userInfo)) {
                return true
            } else {
                return accessControl.checkDocumentToDelete(oldDoc, userInfo, jsonld)
            }
        }
        log.info("No user information received, denying request.")
        return false
    }

    boolean isSystemUser(Map userInfo) {
        if (userInfo.user == "SYSTEM") {
            log.warn("User is SYSTEM. Allowing access to all.")
            return true
        }
    }

    @Deprecated
    List<String> getAlternateIdentifiersFromLinkHeaders(HttpServletRequest request) {
        def alts = []
        for (link in request.getHeaders("Link")) {
            def (id, rel) = link.split(";")*.trim()
            if (rel.replaceAll(/"/, "") == "rel=${SAMEAS_NAMESPACE}") {
                def match = id =~ /<([\S]+)>/
                if (match.matches()) {
                    alts << match[0][1]
                }
            }
        }
        return alts
    }

    String mintIdentifier(Map data) {
        String id = null
        if (data) {
            id = JsonLd.findIdentifier(data)
        }
        return id ?: IdGenerator.generate()
    }


    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("DELETE").inc()
        ongoingRequests.labels("DELETE").inc()
        Summary.Timer requestTimer = requestsLatency.labels("DELETE").startTimer()
        log.info("Handling DELETE request for ${request.pathInfo}")

        try {
            doDelete2(request, response)
        } finally {
            ongoingRequests.labels("DELETE").dec()
            requestTimer.observeDuration()
            log.info("Sending DELETE response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doDelete2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_METHOD_NOT_ALLOWED.toString()).inc()
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed.")
            return
        }
        try {
            String id = getRequestPath(request).substring(1)
            Tuple2<Document, String> docAndLocation = getDocumentFromStorage(id)
            Document doc = docAndLocation.first
            String loc = docAndLocation.second

            log.debug("Checking permissions for ${doc}")

            if (!doc && loc) {
                sendRedirect(request, response, loc)
            } else if (!doc) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_NOT_FOUND.toString()).inc()
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Document not found.")
            } else if (doc && !hasDeletePermission(doc, request.getAttribute("user"))) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            } else if (doc && doc.deleted) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_GONE.toString()).inc()
                response.sendError(HttpServletResponse.SC_GONE,
                        "Document has been deleted.")
            } else if(!whelk.storage.getDependers(doc.getShortId()).isEmpty()) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "This record may not be deleted, because it is referenced by other records.")
            } else {
                log.debug("Removing resource at ${doc.getShortId()}")
                whelk.remove(doc.getShortId(), "xl", null, LegacyIntegrationTools.determineLegacyCollection(doc, jsonld))
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)
            }
        } catch (ModelValidationException mve) {
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    mve.getMessage())
        } catch (Exception wre) {
            log.error("Something went wrong", wre)
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
        }

    }

}
