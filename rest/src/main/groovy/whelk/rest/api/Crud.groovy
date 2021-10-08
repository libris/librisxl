package whelk.rest.api

import groovy.transform.PackageScope
import groovy.util.logging.Log4j2 as Log
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary
import org.apache.http.entity.ContentType
import whelk.Document
import whelk.IdGenerator
import whelk.IdType
import whelk.JsonLd
import whelk.JsonLdValidator
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.exception.ElasticIOException
import whelk.exception.InvalidQueryException
import whelk.exception.LinkValidationException
import whelk.exception.ModelValidationException
import whelk.exception.StaleUpdateException
import whelk.exception.StorageCreateFailedException
import whelk.exception.UnexpectedHttpStatusException
import whelk.exception.WhelkRuntimeException
import whelk.rest.api.CrudGetRequest.Lens
import whelk.rest.security.AccessControl
import whelk.util.LegacyIntegrationTools
import whelk.util.WhelkFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.lang.management.ManagementFactory

import static whelk.rest.api.CrudUtils.ETag
import static whelk.rest.api.HttpTools.getBaseUri
import static whelk.rest.api.HttpTools.sendError
import static whelk.rest.api.HttpTools.sendResponse
import static whelk.util.Jackson.mapper

/**
 * Handles all GET/PUT/POST/DELETE requests against the backend.
 */
@Log
class Crud extends HttpServlet {
    final static String XL_ACTIVE_SIGEL_HEADER = 'XL-Active-Sigel'
    final static String CONTEXT_PATH = '/context.jsonld'

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

    final static Map contextHeaders = [
            "bib": "/sys/context/lib.jsonld",
            "auth": "/sys/context/lib.jsonld",
            "hold": "/sys/context/lib.jsonld"
    ]
    Whelk whelk

    Map vocabData
    Map displayData
    JsonLd jsonld
    JsonLdValidator validator

    SearchUtils search
    AccessControl accessControl = new AccessControl()
    ConverterUtils converterUtils
    Map siteConfig
    Map<String, Tuple2<Document, String>> cachedDocs

    Crud() {
        // Do nothing - only here for Tomcat to have something to call
    }

    Crud(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }
        displayData = whelk.displayData
        vocabData = whelk.vocabData

        jsonld = whelk.jsonld
        search = new SearchUtils(whelk)
        validator = JsonLdValidator.from(jsonld)
        converterUtils = new ConverterUtils(whelk)
        siteConfig = mapper.readValue(getClass().classLoader
                .getResourceAsStream("site_config.json").getBytes(), Map)

        cachedDocs = [
                (whelk.vocabContextUri): getDocumentFromStorage(whelk.vocabContextUri, null),
                (whelk.vocabDisplayUri): getDocumentFromStorage(whelk.vocabDisplayUri, null),
                (whelk.vocabUri): getDocumentFromStorage(whelk.vocabUri, null)

        ]
    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())

        // Depending on what site/client we're serving, we might need to add extra query parameters
        // before they're sent further.
        String activeSite = request.getAttribute('activeSite')

        if (activeSite != siteConfig['default_site']) {
            queryParameters.put('_site_base_uri', (String[])[siteConfig['sites'][activeSite]['@id']])
        }

        if (!queryParameters['_statsrepr'] && siteConfig['sites'][activeSite]['statsfind']) {
            queryParameters.put('_statsrepr', (String[])[mapper.writeValueAsString(siteConfig['sites'][activeSite]['statsfind'])])
        }

        if (!queryParameters['_boost'] && siteConfig['sites'][activeSite]['boost']) {
            queryParameters.put('_boost', (String[])[siteConfig['sites'][activeSite]['boost']])
        }

        try {
            Map results = search.doSearch(queryParameters)
            String responseContentType = CrudUtils.getBestContentType(request)
            if (responseContentType == MimeTypes.JSONLD && !results[JsonLd.CONTEXT_KEY]) {
                results[JsonLd.CONTEXT_KEY] = CONTEXT_PATH
            }
            def jsonResult = mapper.writeValueAsString(results)
            sendResponse(response, jsonResult, responseContentType)
        } catch (ElasticIOException | UnexpectedHttpStatusException e) {
            log.error("Attempted elastic query, but failed: $e", e)
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed to reach elastic for query.")
        }
        catch (WhelkRuntimeException e) {
            log.error("Attempted elastic query, but whelk has no " +
                    "elastic component configured.", e)
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_NOT_IMPLEMENTED.toString()).inc()
            sendError(response, HttpServletResponse.SC_NOT_IMPLEMENTED,
                    "Attempted to use elastic for query, but " +
                            "no elastic component is configured.")
        } catch (InvalidQueryException e) {
            log.warn("Invalid query: ${queryParameters}")
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid query, please check the documentation. ${e.getMessage()}")
        }
    }

    void handleData(HttpServletRequest request, HttpServletResponse response) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String activeSite = request.getAttribute('activeSite')
        Map results = siteConfig['sites'][activeSite]

        if (!queryParameters['_statsrepr']) {
            queryParameters.put('_statsrepr', (String[])[mapper.writeValueAsString(siteConfig['sites'][activeSite]['statsindex'])])
        }
        if (!queryParameters['_limit']) {
            queryParameters.put('_limit', (String[])["0"])
        }
        if (!queryParameters['q']) {
            queryParameters.put('q', (String[])["*"])
        }
        Map searchResults = search.doSearch(queryParameters)
        results['statistics'] = searchResults['stats']

        String responseContentType = CrudUtils.getBestContentType(request)
        if (responseContentType == MimeTypes.JSONLD && !results[JsonLd.CONTEXT_KEY]) {
            results[JsonLd.CONTEXT_KEY] = CONTEXT_PATH
        }
        def jsonResult = mapper.writeValueAsString(results)
        sendResponse(response, jsonResult, responseContentType)
    }

    static void displayInfo(HttpServletResponse response) {
        def info = [:]
        info["system"] = "LIBRISXL"
        info["format"] = "linked-data-api"
        sendResponse(response, mapper.writeValueAsString(info), "application/json")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("GET").inc()
        ongoingRequests.labels("GET").inc()
        Summary.Timer requestTimer = requestsLatency.labels("GET").startTimer()
        log.debug("Handling GET request for ${request.pathInfo}")
        try {
            doGet2(request, response)
        } catch (Exception e) {
            failedRequests.labels("GET", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
        } finally {
            ongoingRequests.labels("GET").dec()
            requestTimer.observeDuration()
            log.debug("Sending GET response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doGet2(HttpServletRequest request, HttpServletResponse response) {
        request.setAttribute('activeSite', getActiveSite(request, getBaseUri(request)))
        log.debug("Active site: ${request.getAttribute('activeSite')}")

        if (request.pathInfo == "/") {
            displayInfo(response)
            return
        }

        // TODO: Handle things other than JSON / JSON-LD
        if (request.pathInfo == "/data" || request.pathInfo == "/data.json" || request.pathInfo == "/data.jsonld") {
            handleData(request, response)
            return
        }

        // TODO: Handle things other than JSON / JSON-LD
        if (request.pathInfo == "/find" || request.pathInfo == "/find.json" || request.pathInfo == "/find.jsonld") {
            handleQuery(request, response)
            return
        }

        def marcframePath = "/sys/marcframe.json"
        if (request.pathInfo == marcframePath) {
            def responseBody = getClass().classLoader.getResourceAsStream("ext/marcframe.json").getText("utf-8")
            sendGetResponse(response, responseBody, ETag.SYSTEM_START, marcframePath, "application/json")
            return
        }
        
        try {
            String activeSite = request.getAttribute('activeSite')
            if (siteConfig['sites'][activeSite]?.get('applyInverseOf', false)) {
                request.setAttribute('_applyInverseOf', "true")
            }

            handleGetRequest(CrudGetRequest.parse(request), response)
        } catch (UnsupportedContentTypeException e) {
            failedRequests.labels("GET", request.getRequestURI(),
                    response.SC_NOT_ACCEPTABLE.toString()).inc()
            sendError(response, HttpServletResponse.SC_NOT_ACCEPTABLE, e.message)
        } catch (NotFoundException e) {
            failedRequests.labels("GET", request.getRequestURI(),
                    response.SC_NOT_FOUND.toString()).inc()
            sendError(response, HttpServletResponse.SC_NOT_FOUND, e.message)
        } catch (BadRequestException e) {
            failedRequests.labels("GET", request.getRequestURI(),
                    response.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, e.message)
        }
        catch (WhelkRuntimeException e) {
            failedRequests.labels("GET", request.getRequestURI(),
                    response.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
        }
    }
    
    void handleGetRequest(CrudGetRequest request,
                          HttpServletResponse response) {
        Tuple2<Document, String> docAndLocation

        if (request.getId() in cachedDocs) {
            docAndLocation = cachedDocs[request.getId()]
        } else {
            docAndLocation = getDocumentFromStorage(
                    request.getId(), request.getVersion().orElse(null))
        }

        Document doc = docAndLocation.v1
        String loc = docAndLocation.v2

        if (!doc && !loc) {
            sendNotFound(response, request.getPath())
        } else if (!doc && loc) {
            sendRedirect(request.getHttpServletRequest(), response, loc)
        } else if (doc.deleted) {
            failedRequests.labels("GET", request.getPath(),
                    HttpServletResponse.SC_GONE.toString()).inc()
            sendError(response, HttpServletResponse.SC_GONE, "Document has been deleted.")
        } else {
            String checksum = doc.getChecksum(jsonld)
            
            if (request.shouldEmbellish()) {
                whelk.embellish(doc)

                // reverse links are inserted by embellish, so can only do this when embellished
                if (request.shouldApplyInverseOf()) {
                    doc.applyInverses(whelk.jsonld)
                }
            }
            
            ETag eTag = request.shouldEmbellish()
                    ? ETag.embellished(checksum, doc.getChecksum(jsonld))
                    : ETag.plain(checksum)

            if (request.getIfNoneMatch().map(eTag.&isNotModified).orElse(false)) {
                sendNotModified(response, eTag)
                return
            }
            
            sendGetResponse(
                    maybeAddProposal25Headers(response, loc),
                    getFormattedResponseBody(request, doc),
                    eTag,
                    request.getPath(),
                    request.getContentType(),
                    request.getId())
        }
    }

    private void sendNotModified(HttpServletResponse response, ETag eTag) {
        setVary(response)
        response.setHeader("ETag", eTag.toString())
        response.setHeader("Server-Start-Time", "" + ManagementFactory.getRuntimeMXBean().getStartTime())
        sendError(response, HttpServletResponse.SC_NOT_MODIFIED, "Document has not been modified.")
    }

    private static void sendNotFound(HttpServletResponse response, String path) {
        failedRequests.labels("GET", path,
                HttpServletResponse.SC_NOT_FOUND.toString()).inc()
        sendError(response, HttpServletResponse.SC_NOT_FOUND, "Document not found.")
    }

    private Object getFormattedResponseBody(CrudGetRequest request, Document doc) {
        log.debug("Formatting document {}. embellished: {}, framed: {}, lens: {}",
                doc.getCompleteId(), request.shouldEmbellish(), request.shouldFrame(), request.getLens())

        def transformedResponse
        if (request.getLens() != Lens.NONE) {
            transformedResponse = applyLens(frameThing(doc), request.getLens())
        } else {
            transformedResponse = request.shouldFrame()  ? frameRecord(doc) : doc.data
        }

        if (!(request.getContentType() in [MimeTypes.JSON, MimeTypes.JSONLD])) {
            def id = request.getContentType() == MimeTypes.TRIG ? doc.getCompleteId() : doc.getShortId()
            return converterUtils.convert(transformedResponse, id, request.getContentType())
        }

        return transformedResponse
    }

    private static Map frameRecord(Document document) {
        return JsonLd.frame(document.getCompleteId(), document.data)
    }

    private static Map frameThing(Document document) {
        document.setThingMeta(document.getCompleteId())
        List<String> thingIds = document.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            String msg = "Could not frame document. Missing mainEntity? In: " + document.getCompleteId()
            log.warn(msg)
            throw new WhelkRuntimeException(msg)
        }
        return JsonLd.frame(thingIds.get(0), document.data)
     }


    private Object applyLens(Object framedThing, Lens lens) {
        switch (lens) {
            case Lens.NONE:
                return framedThing
            case Lens.CARD:
                return whelk.jsonld.toCard(framedThing)
            case Lens.CHIP:
                return whelk.jsonld.toChip(framedThing)
        }
    }

    private static void setVary(HttpServletResponse response) {
        response.setHeader("Vary", "Accept")
    }

    /**
     * Return request URI
     *
     */
    @PackageScope
    static String getRequestPath(HttpServletRequest request) {
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

    private String getActiveSite(HttpServletRequest request, String baseUri = null) {
        // If ?_site=<foo> has been specified (and <foo> is a valid site) it takes precedence
        if (request.getParameter("_site") in siteConfig['sites']) {
            return request.getParameter("_site")
        }

        if (baseUri in siteConfig['sites']) {
            return siteConfig['sites'][baseUri]['@id']
        }

        if (baseUri in siteConfig['site_alias']) {
            String actualSite = siteConfig['site_alias'][baseUri]
            return siteConfig['sites'][actualSite]['@id']
        }

        return siteConfig['default_site']
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

    private static HttpServletResponse maybeAddProposal25Headers(HttpServletResponse response,
                                                                 String location) {
        if (location) {
            response.addHeader('Content-Location',
                               getDataURI(location))
            response.addHeader('Document', location)
            response.addHeader('Link', "<${location}>; rel=describedby")
        }
        return response
    }

    private static String getDataURI(String location) {
        if (location.endsWith('/')) {
            return location + 'data.jsonld'
        } else {
            return location + '/data.jsonld'
        }
    }

    /**
     * Format and send GET response to client.
     *
     * Sets the necessary headers and picks the best Content-Type to use.
     *
     */
    void sendGetResponse(HttpServletResponse response,
                         Object responseBody, ETag eTag, String path,
                         String contentType, String requestId = null) {
        // FIXME remove?
        String ctxHeader = contextHeaders.get(path.split("/")[1])
        if (ctxHeader) {
            response.setHeader("Link",
                    "<$ctxHeader>; " +
                            "rel=\"http://www.w3.org/ns/json-ld#context\"; " +
                            "type=\"application/ld+json\"")
        }

        if (contentType == MimeTypes.JSONLD && responseBody instanceof Map && requestId != whelk.vocabContextUri) {
            responseBody[JsonLd.CONTEXT_KEY] = CONTEXT_PATH
        }

        response.setHeader("ETag", eTag.toString())

        if (path in contextHeaders.collect { it.value }) {
            log.debug("request is for context file. " +
                    "Must serve original content-type ($contentType).")
            // FIXME what should happen here?
        }

        setVary(response)

        sendResponse(response, responseBody, contentType)
    }

    /**
     * Send 302 Found response
     *
     */
    static void sendRedirect(HttpServletRequest request,
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
        log.debug("Handling POST request for ${request.pathInfo}")

        try {
            doPost2(request, response)
        } catch (Exception e) {
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
        } finally {
            ongoingRequests.labels("POST").dec()
            requestTimer.observeDuration()
            log.debug("Sending POST response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doPost2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo != "/") {
            log.debug("Invalid POST request URL.")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_METHOD_NOT_ALLOWED.toString()).inc()
            sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed.")
            return
        }

        if (!isSupportedContentType(request)) {
            log.debug("Unsupported Content-Type for POST.")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Content-Type not supported.")
            return
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            log.debug("Empty POST request.")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "No data received")
            return
        }

        if (!JsonLd.isFlat(requestBody)) {
            log.debug("POST body is not flat JSON-LD")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Body is not flat JSON-LD.")
            return
        }

        // FIXME we're assuming Content-Type application/ld+json here
        // should we deny the others?

        Document newDoc = new Document(requestBody)

        if (!newDoc.getId()) {
            log.debug("Temporary @id missing in Record")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Document is missing temporary @id in Record")
            return
        }

        if (newDoc.getThingIdentifiers().isEmpty()) {
            log.debug("Temporary mainEntity @id missing in Record")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Document is missing temporary mainEntity.@id in Record")
            return
        }

        if (!newDoc.data['@graph'][1] || !newDoc.data['@graph'][1]['@id']) {
            log.debug("Temporary @id missing in Thing")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Document is missing temporary @id in Thing")
            return
        }

        if (newDoc.getThingIdentifiers().first() != newDoc.data['@graph'][1]['@id']) {
            log.debug("mainEntity.@id not same as Thing @id")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "The Record's temporary mainEntity.@id is not same as the Thing's temporary @id")
            return
        }

        if (newDoc.getId() == newDoc.getThingIdentifiers().first()) {
            log.debug("Record @id same as Thing @id")
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "The Record's temporary @id can't be the same as the Thing's temporary mainEntity.@id")
            return
        }

        newDoc.normalizeUnicode()
        newDoc.deepReplaceId(Document.BASE_URI.toString() + IdGenerator.generate())
        newDoc.setControlNumber(newDoc.getShortId())

        String collection = LegacyIntegrationTools.determineLegacyCollection(newDoc, jsonld)
        List<JsonLdValidator.Error> errors = validator.validate(newDoc.data, collection)
        if (errors) {
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid JSON-LD", ['errors': errors.collect{ it.toMap() }])
            return
        }
        
        // verify user permissions
        log.debug("Checking permissions for ${newDoc}")
        try {
            boolean allowed = hasPostPermission(newDoc, request.getAttribute("user"))
            if (!allowed) {
                failedRequests.labels("POST", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                sendError(response, HttpServletResponse.SC_FORBIDDEN,
                        "You are not authorized to perform this " +
                                "operation")
                log.debug("Permission check failed. Denying request.")
                return
            }
        } catch (ModelValidationException mve) {
            failedRequests.labels("POST", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            // FIXME data leak
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, mve.getMessage())
            log.debug("Model validation check failed. Denying request: " + mve.getMessage())
            return
        }
        
        // try store document
        // return 201 or error
        boolean isUpdate = false
        Document savedDoc
        savedDoc = saveDocument(newDoc, request, response, isUpdate, "POST")
        if (savedDoc != null) {
            sendCreateResponse(response, savedDoc.getURI().toString(),
                               savedDoc.getChecksum(jsonld))
        } else if (!response.isCommitted()) {
            sendNotFound(response, request.getContextPath())
        }
    }

    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("PUT").inc()
        ongoingRequests.labels("PUT").inc()
        Summary.Timer requestTimer = requestsLatency.labels("PUT").startTimer()
        log.debug("Handling PUT request for ${request.pathInfo}")

        try {
            doPut2(request, response)
        } catch (Exception e) {
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
        } finally {
            ongoingRequests.labels("PUT").dec()
            requestTimer.observeDuration()
            log.debug("Sending PUT response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }

    }

    void doPut2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            log.debug("Invalid PUT request URL.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_METHOD_NOT_ALLOWED.toString()).inc()
            sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                    "Method not allowed.")
            return
        }

        if (!isSupportedContentType(request)) {
            log.debug("Unsupported Content-Type for PUT.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Content-Type not supported.")
            return
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            log.debug("Empty PUT request.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "No data received")
            return
        }

        String documentId = JsonLd.findIdentifier(requestBody)
        String idFromUrl = getRequestPath(request).substring(1)

        if (!documentId) {
            log.debug("Missing document ID in PUT request.")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Missing @id in request.")
            return
        }

        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(idFromUrl)
        Document existingDoc = docAndLoc.v1
        String location = docAndLoc.v2

        if (!existingDoc && !location) {
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_NOT_FOUND.toString()).inc()
            sendError(response, HttpServletResponse.SC_NOT_FOUND,
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
                sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                        "Record ID was modified")
                return
            }
        }

        Document updatedDoc = new Document(requestBody)
        updatedDoc.normalizeUnicode()
        updatedDoc.setId(documentId)
                
        String collection = LegacyIntegrationTools.determineLegacyCollection(updatedDoc, jsonld)
        List<JsonLdValidator.Error> errors = validator.validate(updatedDoc.data, collection)
        if (errors) {
            String message = errors.collect { it.toStringWithPath() }.join("\n")
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid JsonLd, got errors: " + message)
            return
        }

        log.debug("Checking permissions for ${updatedDoc}")
        try {
            // TODO: 'collection' must also match the collection 'existingDoc'
            // is in.
            boolean allowed = hasPutPermission(updatedDoc, existingDoc,
                    (Map) request.getAttribute("user"))
            if (!allowed) {
                failedRequests.labels("PUT", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                sendError(response, HttpServletResponse.SC_FORBIDDEN,
                        "You are not authorized to perform this " +
                                "operation")
                return
            }
        } catch (ModelValidationException mve) {
            failedRequests.labels("PUT", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            // FIXME data leak
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    mve.getMessage())
            return
        }
        
        boolean isUpdate = true
        Document savedDoc = saveDocument(updatedDoc, request, response, isUpdate, "PUT")
        if (savedDoc != null) {
            sendUpdateResponse(response, savedDoc.getURI().toString(),
                               savedDoc.getChecksum(jsonld))
        }

    }

    static boolean isEmptyInput(Map inputData) {
        return !inputData || inputData.size() == 0
    }

    static boolean isSupportedContentType(HttpServletRequest request) {
        ContentType contentType = ContentType.parse(request.getContentType())
        String mimeType = contentType.getMimeType()
        // FIXME add additional content types?
        return mimeType == "application/ld+json"
    }

    static Map getRequestBody(HttpServletRequest request) {
        byte[] body = request.getInputStream().getBytes()

        try {
            return mapper.readValue(body, Map)
        } catch (EOFException) {
            return [:]
        }
    }

    Document saveDocument(Document doc, HttpServletRequest request,
                          HttpServletResponse response,
                          boolean isUpdate, String httpMethod) {
        try {
            if (doc) {
                String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER)
                String collection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
                
                if (doc.isCacheRecord()) {
                    throw new BadRequestException("Cannot POST/PUT cache record")
                }
                
                if (isUpdate) {

                    // You are not allowed to change collection when updating a record
                    if (collection != whelk.storage.getCollectionBySystemID(doc.getShortId())) {
                        log.warn("Refused API update of document due to changed 'collection'")
                        throw new BadRequestException("Cannot change legacy collection for document. Legacy collection is mapped from entity @type.")
                    }

                    ETag ifMatch = Optional
                            .ofNullable(request.getHeader("If-Match"))
                            .map(ETag.&parse)
                            .orElseThrow({ -> new BadRequestException("Missing If-Match header in update") })
                    
                    log.info("If-Match: ${ifMatch}")
                    whelk.storeAtomicUpdate(doc, false, "xl", activeSigel, ifMatch.documentCheckSum())
                }
                else {
                    log.debug("Saving NEW document ("+ doc.getId() +")")
                    boolean success = whelk.createDocument(doc, "xl", activeSigel, collection, false)
                    if (!success) {
                        return null
                    }
                }

                log.debug("Saving document (${doc.getShortId()})")
                log.info("Document accepted: created is: ${doc.getCreated()}")

                return doc
            }
        } catch (StorageCreateFailedException scfe) {
            log.warn("Refused document with id ${scfe.duplicateId}")
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_CONFLICT.toString()).inc()
            sendError(response, HttpServletResponse.SC_CONFLICT,
                    scfe.message)
            return null
        } catch (StaleUpdateException eme) {
            log.warn("Did not store document, because the ETAGs did not match.")
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_PRECONDITION_FAILED.toString()).inc()
            sendError(response, HttpServletResponse.SC_PRECONDITION_FAILED,
                                        "The resource has been updated by someone " +
                                                "else. Please refetch.")
            return null
        } catch (PostgreSQLComponent.AcquireLockException e) {
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Failed to acquire a necessary lock. Did you submit a holding record without a valid bib link? " + e.message)
            return null
        } catch (LinkValidationException | PostgreSQLComponent.ConflictingHoldException | BadRequestException e) {
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
            return null
        } catch (Exception e) {
            failedRequests.labels(httpMethod, request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            // FIXME data leak
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
            return null
        }
        return null
    }

    static void sendCreateResponse(HttpServletResponse response, String locationRef,
                                   String etag) {
        sendDocumentSavedResponse(response, locationRef, etag, true)
    }

    static void sendUpdateResponse(HttpServletResponse response, String locationRef,
                                   String etag) {
        sendDocumentSavedResponse(response, locationRef, etag, false)
    }

    static void sendDocumentSavedResponse(HttpServletResponse response,
                                          String locationRef, String etag,
                                          boolean newDocument) {
        log.debug("Setting header Location: $locationRef")

        response.setHeader("Location", locationRef)
        response.setHeader("ETag", "\"${etag as String}\"")
        response.setHeader('Cache-Control', 'no-cache')

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

    static boolean isSystemUser(Map userInfo) {
        if (userInfo.user == "SYSTEM") {
            log.warn("User is SYSTEM. Allowing access to all.")
            return true
        }

        return false
    }

    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("DELETE").inc()
        ongoingRequests.labels("DELETE").inc()
        Summary.Timer requestTimer = requestsLatency.labels("DELETE").startTimer()
        log.debug("Handling DELETE request for ${request.pathInfo}")

        try {
            doDelete2(request, response)
        } catch (Exception e) {
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
        } finally {
            ongoingRequests.labels("DELETE").dec()
            requestTimer.observeDuration()
            log.debug("Sending DELETE response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doDelete2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_METHOD_NOT_ALLOWED.toString()).inc()
            sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed.")
            return
        }
        try {
            String id = getRequestPath(request).substring(1)
            Tuple2<Document, String> docAndLocation = getDocumentFromStorage(id)
            Document doc = docAndLocation.v1
            String loc = docAndLocation.v2

            log.debug("Checking permissions for ${doc}")

            if (!doc && loc) {
                sendRedirect(request, response, loc)
            } else if (!doc) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_NOT_FOUND.toString()).inc()
                sendError(response, HttpServletResponse.SC_NOT_FOUND, "Document not found.")
            } else if (doc && !hasDeletePermission(doc, request.getAttribute("user"))) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                sendError(response, HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            } else if (doc && doc.deleted) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_GONE.toString()).inc()
                sendError(response, HttpServletResponse.SC_GONE,
                        "Document has been deleted.")
            } else if(!whelk.storage.followDependers(doc.getShortId()).isEmpty()) {
                failedRequests.labels("DELETE", request.getRequestURI(),
                        HttpServletResponse.SC_FORBIDDEN.toString()).inc()
                sendError(response, HttpServletResponse.SC_FORBIDDEN, "This record may not be deleted, because it is referenced by other records.")
            } else {
                log.debug("Removing resource at ${doc.getShortId()}")
                String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER)
                whelk.remove(doc.getShortId(), "xl", activeSigel)
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)
            }
        } catch (ModelValidationException mve) {
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_BAD_REQUEST.toString()).inc()
            // FIXME data leak
            sendError(response, HttpServletResponse.SC_BAD_REQUEST,
                    mve.getMessage())
        } catch (Exception e) {
            failedRequests.labels("DELETE", request.getRequestURI(),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR.toString()).inc()
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage(), e)
        }

    }
    
    static class NotFoundException extends RuntimeException {
        NotFoundException(String msg) {
            super(msg)
        }
    }
}
