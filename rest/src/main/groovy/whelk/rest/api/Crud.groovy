package whelk.rest.api

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Log4j2 as Log
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.Summary
import org.apache.http.entity.ContentType
import whelk.Document
import whelk.IdGenerator
import whelk.IdType
import whelk.JsonLd
import whelk.JsonLdValidator
import whelk.TargetVocabMapper
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
import whelk.history.History
import whelk.rest.api.CrudGetRequest.Lens
import whelk.rest.security.AccessControl
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
@CompileStatic
class Crud extends HttpServlet {
    final static String XL_ACTIVE_SIGEL_HEADER = 'XL-Active-Sigel'
    final static String CONTEXT_PATH = '/context.jsonld'
    final static String DATA_CONTENT_TYPE = "application/ld+json"

    static final Counter requests = Counter.build()
        .name("api_requests_total").help("Total requests to API.")
        .labelNames("method").register()

    static final Counter failedRequests = Counter.build()
        .name("api_failed_requests_total").help("Total failed requests to API.")
        .labelNames("method", "status").register()

    static final Gauge ongoingRequests = Gauge.build()
        .name("api_ongoing_requests_total").help("Total ongoing API requests.")
        .labelNames("method").register()

    static final Summary requestsLatency = Summary.build()
        .name("api_requests_latency_seconds")
        .help("API request latency in seconds.")
        .labelNames("method")
        .quantile(0.5f, 0.05f)
        .quantile(0.95f, 0.01f)
        .quantile(0.99f, 0.001f)
        .register()

    static final Histogram requestsLatencyHistogram = Histogram.build()
            .name("api_requests_latency_seconds_histogram").help("API request latency in seconds.")
            .labelNames("method")
            .register()

    Whelk whelk

    Map vocabData
    Map displayData
    JsonLd jsonld
    JsonLdValidator validator
    TargetVocabMapper targetVocabMapper

    SearchUtils search
    AccessControl accessControl = new AccessControl()
    ConverterUtils converterUtils

    Map siteConfig
    Map sitesData
    Map siteAlias

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
        sitesData = (Map) siteConfig['sites']
        siteAlias = (Map) siteConfig['site_alias']

        cachedDocs = [
                (whelk.vocabContextUri): getDocumentFromStorage(whelk.vocabContextUri, null),
                (whelk.vocabDisplayUri): getDocumentFromStorage(whelk.vocabDisplayUri, null),
                (whelk.vocabUri): getDocumentFromStorage(whelk.vocabUri, null)

        ]
        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(whelk.kbvContextUri)
        Document contextDoc = docAndLoc.v1
        if (contextDoc) {
            targetVocabMapper = new TargetVocabMapper(whelk.jsonld, contextDoc.data)
        }
    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())

        // Depending on what site/client we're serving, we might need to add extra query parameters
        // before they're sent further.
        String activeSite = request.getAttribute('activeSite')
        Map activeSiteData = (Map) sitesData[activeSite]

        if (activeSite != siteConfig['default_site']) {
            queryParameters.put('_site_base_uri', [activeSiteData['@id']] as String[])
        }

        if (!queryParameters['_statsrepr'] && activeSiteData['statsfind']) {
            queryParameters.put('_statsrepr', [mapper.writeValueAsString(activeSiteData['statsfind'])] as String[])
        }

        if (!queryParameters['_boost'] && activeSiteData['boost']) {
            queryParameters.put('_boost', [activeSiteData['boost']] as String[])
        }

        try {
            Map results = search.doSearch(queryParameters)
            String uri = request.getRequestURI()
            Map contextData = whelk.jsonld.context
            def crudReq = CrudGetRequest.parse(request)
            def dataBody = getNegotiatedDataBody(crudReq, contextData, results, uri)

            sendGetResponse(response, dataBody, null, crudReq.getPath(), crudReq.getContentType(), crudReq.getId())
        } catch (ElasticIOException | UnexpectedHttpStatusException e) {
            log.error("Attempted elastic query, but failed: $e", e)
            throw new WhelkRuntimeException("Failed to reach elastic for query.", e)
        } catch (WhelkRuntimeException e) {
            log.error("Attempted elastic query, but whelk has no elastic component configured.", e)
            throw new OtherStatusException("Attempted to use elastic for query, but no elastic component is configured.",
                    HttpServletResponse.SC_NOT_IMPLEMENTED)
        } catch (InvalidQueryException e) {
            log.warn("Invalid query: ${queryParameters}")
            throw new BadRequestException("Invalid query, please check the documentation. ${e.getMessage()}")
        }
    }

    void handleData(HttpServletRequest request, HttpServletResponse response) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String activeSite = request.getAttribute('activeSite')
        Map results = [:]
        results.putAll((Map) sitesData[activeSite])

        if (!queryParameters['_statsrepr']) {
            queryParameters.put('_statsrepr', [mapper.writeValueAsString(sitesData[activeSite]['statsindex'])] as String[])
        }
        if (!queryParameters['_limit']) {
            queryParameters.put('_limit', ["0"] as String[])
        }
        if (!queryParameters['q']) {
            queryParameters.put('q', ["*"] as String[])
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
        String metricLabel = isFindRequest(request) ? 'FIND' : 'GET'
        requests.labels(metricLabel).inc()
        ongoingRequests.labels(metricLabel).inc()
        Summary.Timer requestTimer = requestsLatency.labels(metricLabel).startTimer()
        Histogram.Timer requestTimer2 = requestsLatencyHistogram.labels(metricLabel).startTimer()
        
        log.debug("Handling GET request for ${request.pathInfo}")
        try {
            doGet2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            ongoingRequests.labels(metricLabel).dec()
            requestTimer.observeDuration()
            requestTimer2.observeDuration()
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

        if (isFindRequest(request)) {
            handleQuery(request, response)
            return
        }

        def marcframePath = "/sys/marcframe.json"
        if (request.pathInfo == marcframePath) {
            def responseBody = getClass().classLoader.getResourceAsStream("ext/marcframe.json").getText("utf-8")
            sendGetResponse(response, responseBody, ETag.SYSTEM_START, marcframePath, "application/json")
            return
        }

        String activeSite = request.getAttribute('activeSite')
        Map activeSiteData = (Map) sitesData[activeSite]
        if (activeSiteData?.getOrDefault('applyInverseOf', false)) {
            request.setAttribute('_applyInverseOf', "true")
        }

        handleGetRequest(CrudGetRequest.parse(request), response)
    }
    
    private boolean isFindRequest(HttpServletRequest request) {
        request.pathInfo == "/find" || request.pathInfo.startsWith("/find.")
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
            sendNotFound(request, response)
        } else if (!doc && loc) {
            if (request.getView() == CrudGetRequest.View.DATA) {
                loc = getDataURI(loc, request.contentType)
            }
            sendRedirect(request.getHttpServletRequest(), response, loc)
        } else if (doc.deleted) {
            throw new OtherStatusException("Document has been deleted.", HttpServletResponse.SC_GONE)
        } else if (request.getView() == CrudGetRequest.View.CHANGE_SETS) {
            History history = new History(whelk.storage.loadDocumentHistory(doc.getShortId()), jsonld)
            ETag eTag = ETag.plain(doc.getChecksum(jsonld))
            def body = history.m_changeSetsMap
            sendGetResponse(response, body, eTag, request.getPath(), request.getContentType(), request.getId())
        } else {
            ETag eTag
            if (request.shouldEmbellish()) {
                String plainChecksum = doc.getChecksum(jsonld)
                whelk.embellish(doc)
                eTag = ETag.embellished(plainChecksum, doc.getChecksum(jsonld))

                // reverse links are inserted by embellish, so can only do
                // this when embellished
                if (request.shouldApplyInverseOf()) {
                    doc.applyInverses(whelk.jsonld)
                }
            } else {
                eTag = ETag.plain(doc.getChecksum(jsonld))
            }

            if (request.getIfNoneMatch().map(eTag.&isNotModified).orElse(false)) {
                sendNotModified(response, eTag)
                return
            }

            String profileId = request.getProfile().orElse(whelk.defaultTvmProfile)
            addProfileHeaders(response, profileId)
            def body = getFormattedResponseBody(request, doc, profileId)

            String location = loc ?: doc.id
            addProposal25Headers(response, location, getDataURI(location, request))

            sendGetResponse(response, body, eTag, request.getPath(), request.getContentType(), request.getId())
        }
    }

    private void sendNotModified(HttpServletResponse response, ETag eTag) {
        setVary(response)
        response.setHeader("ETag", eTag.toString())
        response.setHeader("Server-Start-Time", "" + ManagementFactory.getRuntimeMXBean().getStartTime())
        sendError(response, HttpServletResponse.SC_NOT_MODIFIED, "Document has not been modified.")
    }

    private static void sendNotFound(HttpServletRequest request, HttpServletResponse response) {
        failedRequests.labels(request.getMethod(), HttpServletResponse.SC_NOT_FOUND.toString()).inc()
        sendError(response, HttpServletResponse.SC_NOT_FOUND, "Document not found.")
    }

    private Object getFormattedResponseBody(CrudGetRequest request, Document doc, String profileId) {
        log.debug("Formatting document {}. embellished: {}, framed: {}, lens: {}, view: {}, profile: {}",
                doc.getCompleteId(),
                request.shouldEmbellish(),
                request.shouldFrame(),
                request.getLens(),
                request.getView(),
                profileId)

        Map data
        if (request.getLens() != Lens.NONE) {
            data = applyLens(frameThing(doc), request.getLens())
        } else {
            data = request.shouldFrame()  ? frameRecord(doc) : doc.data
        }

        Object contextData = whelk.jsonld.context
        if (profileId != whelk.defaultTvmProfile) {
            data = applyDataProfile(profileId, data)
            contextData = data[JsonLd.CONTEXT_KEY]
            data[JsonLd.CONTEXT_KEY] = profileId
        }


        String uri = request.getContentType() == MimeTypes.TRIG ? doc.getCompleteId() : doc.getShortId()
        return getNegotiatedDataBody(request, contextData, data, uri)
    }

    private Object getNegotiatedDataBody(CrudGetRequest request, Object contextData, Map data, String uri) {
        if (!(request.getContentType() in [MimeTypes.JSON, MimeTypes.JSONLD])) {
            data[JsonLd.CONTEXT_KEY] = contextData
            return converterUtils.convert(data, uri, request.getContentType())
        } else {
            return data
        }
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
    
    private Map applyLens(Map framedThing, Lens lens) {
        switch (lens) {
            case Lens.NONE:
                return framedThing
            case Lens.CARD:
                return whelk.jsonld.toCard(framedThing)
            case Lens.CHIP:
                return (Map) whelk.jsonld.toChip(framedThing)
        }
    }

    private static void setVary(HttpServletResponse response) {
        response.setHeader("Vary", "Accept, Accept-Profile")
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
        if (request.getParameter("_site") in sitesData) {
            return request.getParameter("_site")
        }

        if (baseUri in siteAlias) {
            baseUri = siteAlias[baseUri]
        }

        if (baseUri in sitesData) {
            return sitesData[baseUri]['@id']
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

    /**
     * See <https://www.w3.org/wiki/HTML/ChangeProposal25>
     */
    private static void addProposal25Headers(HttpServletResponse response,
                                             String location,
                                             String dataLocation) {
        response.addHeader('Content-Location', dataLocation)
        response.addHeader('Document', location)
        response.addHeader('Link', "<${location}>; rel=describedby")
    }

    /**
     * TODO: Spec in flux; see status at:
     * <https://www.w3.org/TR/dx-prof-conneg/>
     * and:
     * <https://profilenegotiation.github.io/I-D-Profile-Negotiation/I-D-Profile-Negotiation.html>
     */
    private static void addProfileHeaders(HttpServletResponse response, String profileId) {
        response.setHeader("Content-Profile", "<$profileId>")
        response.setHeader("Link", "<$profileId>; rel=\"profile\"")
    }

    private static String getDataURI(String location, CrudGetRequest request) {
        String lens = request.lens == Lens.NONE ? null : request.lens.toString().toLowerCase()
        return getDataURI(location, request.contentType, lens, request.profile.orElse(null))
    }

    private static String getDataURI(String location,
                                     String contentType,
                                     String lens=null,
                                     String profile=null) {
        if (contentType == null) {
            return location
        }
        
        if (location.endsWith('#it')) {
            // We should normally never get '#it' URIs here since /data should only work on records 
            location = location.substring(0, location.length() - '#it'.length())
        }
        
        def loc = new StringBuilder(location)

        String slash = location.endsWith('/') ? '' : '/'
        String ext = CrudUtils.EXTENSION_BY_MEDIA_TYPE[contentType] ?: 'jsonld'

        loc << slash << 'data.' << ext

        def params = new StringJoiner("&")
        if (lens != null) {
            params.add("lens=${lens}")
        }
        if (profile != null) {
            params.add("profile=${profile}")
        }
        if (params.length() > 0) {
            loc << '?' << params.toString()
        }

        return loc.toString()
    }

    Map applyDataProfile(String profileId, Map data) {
        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(profileId)
        Document profileDoc = docAndLoc.v1
        if (profileDoc == null) {
            throw new BadRequestException("Profile <${profileId}> is not available")
        }
        log.debug("Using profile: $profileId")
        def contextDoc = profileDoc.data
        data = (Map) targetVocabMapper.applyTargetVocabularyMap(profileId, contextDoc, data)
        data[JsonLd.CONTEXT_KEY] = contextDoc[JsonLd.CONTEXT_KEY]

        return data
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
        if (contentType == MimeTypes.JSON) {
            response.setHeader("Link",
                    "<$CONTEXT_PATH>; " +
                            "rel=\"http://www.w3.org/ns/json-ld#context\"; " +
                            "type=\"application/ld+json\"")
        } else if (contentType == MimeTypes.JSONLD && responseBody instanceof Map && requestId != whelk.vocabContextUri) {
            if (!responseBody.containsKey(JsonLd.CONTEXT_KEY)) {
                responseBody[JsonLd.CONTEXT_KEY] = CONTEXT_PATH
            }
        }

        if (eTag != null) {
            response.setHeader("ETag", eTag.toString())
        }

        setVary(response)

        if (responseBody instanceof Map) {
            sendResponse(response, (Map) responseBody, contentType)
        } else {
            sendResponse(response, (String) responseBody, contentType)
        }
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
        Histogram.Timer requestTimer2 = requestsLatencyHistogram.labels("POST").startTimer()
        log.debug("Handling POST request for ${request.pathInfo}")

        try {
            doPost2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            ongoingRequests.labels("POST").dec()
            requestTimer.observeDuration()
            requestTimer2.observeDuration()
            log.debug("Sending POST response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doPost2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo != "/") {
            throw new OtherStatusException("Method not allowed.", HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
        if (!isSupportedContentType(request.getContentType())) {
            throw new BadRequestException("Content-Type not supported.")
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            throw new BadRequestException("No data received.")
        }
        if (!JsonLd.isFlat(requestBody)) {
            throw new BadRequestException("Body is not flat JSON-LD.")
        }

        // FIXME we're assuming Content-Type application/ld+json here
        // should we deny the others?

        Document newDoc = new Document(requestBody)

        if (!newDoc.getId()) {
            throw new BadRequestException("Document is missing temporary @id in Record")
        }
        if (newDoc.getThingIdentifiers().isEmpty()) {
            throw new BadRequestException("Document is missing temporary mainEntity.@id in Record")
        }

        List graph = (List) newDoc.data['@graph']
        if (!graph[1] || !graph[1]['@id']) {
            throw new BadRequestException("Document is missing temporary @id in Thing")
        }

        if (newDoc.getThingIdentifiers().first() != graph[1]['@id']) {
            throw new BadRequestException("The Record's temporary mainEntity.@id is not same as the Thing's temporary @id")
        }

        if (newDoc.getId() == newDoc.getThingIdentifiers().first()) {
            throw new BadRequestException("The Record's temporary @id can't be the same as te Thing's temporary mainEntity.@id")
        }

        newDoc.normalizeUnicode()
        newDoc.deepReplaceId(Document.BASE_URI.toString() + IdGenerator.generate())
        newDoc.setControlNumber(newDoc.getShortId())
        
        List<JsonLdValidator.Error> errors = validator.validate(newDoc.data, newDoc.getLegacyCollection(jsonld))
        if (errors) {
            throw new BadRequestException("Invalid JSON-LD", ['errors': errors.collect{ it.toMap() }])
        }
        
        // verify user permissions
        if (!hasPostPermission(newDoc, (Map) request.getAttribute("user"))) {
            throw new OtherStatusException("You are not authorized to perform this operation", HttpServletResponse.SC_FORBIDDEN)
        }
        
        // try store document
        // return 201 or error
        boolean isUpdate = false
        Document savedDoc
        savedDoc = saveDocument(newDoc, request, isUpdate)
        if (savedDoc != null) {
            sendCreateResponse(response, savedDoc.getURI().toString(),
                    ETag.plain(savedDoc.getChecksum(jsonld)))
        } else if (!response.isCommitted()) {
            sendNotFound(request, response)
        }
    }

    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        requests.labels("PUT").inc()
        ongoingRequests.labels("PUT").inc()
        Summary.Timer requestTimer = requestsLatency.labels("PUT").startTimer()
        Histogram.Timer requestTimer2 = requestsLatencyHistogram.labels("PUT").startTimer()
        log.debug("Handling PUT request for ${request.pathInfo}")

        try {
            doPut2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            ongoingRequests.labels("PUT").dec()
            requestTimer.observeDuration()
            requestTimer2.observeDuration()
            log.debug("Sending PUT response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }

    }

    void doPut2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            throw new OtherStatusException("Method not allowed.", HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
        if (!isSupportedContentType(request.getContentType())) {
            throw new BadRequestException("Content-Type not supported.")
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            throw new BadRequestException("No data received.")
        }

        String documentId = JsonLd.findIdentifier(requestBody)
        String idFromUrl = getRequestPath(request).substring(1)

        if (!documentId) {
            throw new BadRequestException("Missing @id in request.")
        }

        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(idFromUrl)
        Document existingDoc = docAndLoc.v1
        String location = docAndLoc.v2

        if (!existingDoc && !location) {
            throw new Crud.NotFoundException("Document not found.")
        } else if (!existingDoc && location) {
            sendRedirect(request, response, location)
            return
        } else  {
            String fullPutId = JsonLd.findFullIdentifier(requestBody)
            if (fullPutId != existingDoc.id) {
                log.debug("Record ID for ${existingDoc.id} changed to " +
                          "${fullPutId} in PUT body")
                throw new BadRequestException("Record ID was modified")
            }
        }

        Document updatedDoc = new Document(requestBody)
        updatedDoc.normalizeUnicode()
        updatedDoc.setId(documentId)
        
        List<JsonLdValidator.Error> errors = validator.validate(updatedDoc.data, updatedDoc.getLegacyCollection(jsonld))
        if (errors) {
            throw new BadRequestException("Invalid JSON-LD", ['errors': errors.collect{ it.toMap() }])
        }

        log.debug("Checking permissions for ${updatedDoc}")
        // TODO: 'collection' must also match the collection 'existingDoc' is in.
        if (!hasPutPermission(updatedDoc, existingDoc, (Map) request.getAttribute("user"))) {
            throw new OtherStatusException("You are not authorized to perform this operation", HttpServletResponse.SC_FORBIDDEN)
        }
        
        boolean isUpdate = true
        Document savedDoc = saveDocument(updatedDoc, request, isUpdate)
        if (savedDoc != null) {
            sendUpdateResponse(response, savedDoc.getURI().toString(),
                    ETag.plain(savedDoc.getChecksum(jsonld)))
        }

    }

    static boolean isEmptyInput(Map inputData) {
        return !inputData || inputData.size() == 0
    }

    static boolean isSupportedContentType(String contentType) {
        String mimeType = ContentType.parse(contentType).getMimeType()
        return mimeType == DATA_CONTENT_TYPE
    }

    static Map getRequestBody(HttpServletRequest request) {
        byte[] body = request.getInputStream().getBytes()

        try {
            return mapper.readValue(body, Map)
        } catch (EOFException) {
            return [:]
        }
    }

    Document saveDocument(Document doc, HttpServletRequest request, boolean isUpdate) {
        try {
            if (doc) {
                String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER)
                String collection = doc.getLegacyCollection(jsonld)
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
        } catch (StaleUpdateException ignored) {
            throw new OtherStatusException("The resource has been updated by someone else. Please refetch.", HttpServletResponse.SC_PRECONDITION_FAILED)
        } catch (PostgreSQLComponent.AcquireLockException e) {
            throw new BadRequestException("Failed to acquire a necessary lock. Did you submit a holding record without a valid bib link? " + e.message)
        }
        return null
    }

    static void sendCreateResponse(HttpServletResponse response, String locationRef, ETag eTag) {
        sendDocumentSavedResponse(response, locationRef, eTag, true)
    }

    static void sendUpdateResponse(HttpServletResponse response, String locationRef, ETag eTag) {
        sendDocumentSavedResponse(response, locationRef, eTag, false)
    }

    static void sendDocumentSavedResponse(HttpServletResponse response,
                                          String locationRef, 
                                          ETag eTag,
                                          boolean newDocument) {
        log.debug("Setting header Location: $locationRef")

        response.setHeader("Location", locationRef)
        response.setHeader("ETag", eTag.toString())
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
        Histogram.Timer requestTimer2 = requestsLatencyHistogram.labels("DELETE").startTimer()
        log.debug("Handling DELETE request for ${request.pathInfo}")

        try {
            doDelete2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            ongoingRequests.labels("DELETE").dec()
            requestTimer.observeDuration()
            requestTimer2.observeDuration()
            log.debug("Sending DELETE response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doDelete2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo == "/") {
            throw new OtherStatusException("Method not allowed.", HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
        String id = getRequestPath(request).substring(1)
        Tuple2<Document, String> docAndLocation = getDocumentFromStorage(id)
        Document doc = docAndLocation.v1
        String loc = docAndLocation.v2

        if (!doc && loc) {
            sendRedirect(request, response, loc)
        } else if (!doc) {
            throw new NotFoundException("Document not found.")
        } else if (doc && !hasDeletePermission(doc, (Map) request.getAttribute("user"))) {
            throw new OtherStatusException("You do not have sufficient privileges to perform this operation.", HttpServletResponse.SC_FORBIDDEN)
        } else if (doc && doc.deleted) {
            throw new OtherStatusException("Document has been deleted.", HttpServletResponse.SC_GONE)
        } else if(!whelk.storage.followDependers(doc.getShortId()).isEmpty()) {
            throw new OtherStatusException("This record may not be deleted, because it is referenced by other records.", HttpServletResponse.SC_FORBIDDEN)
        } else {
            log.debug("Removing resource at ${doc.getShortId()}")
            String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER)
            whelk.remove(doc.getShortId(), "xl", activeSigel)
            response.setStatus(HttpServletResponse.SC_NO_CONTENT)
        }
    }

    static void sendError(HttpServletRequest request, HttpServletResponse response, Exception e) {
        int code = mapError(e)
        failedRequests.labels(request.getMethod(), code.toString()).inc()
        if (log.isDebugEnabled()) {
            log.debug("Sending error $code : ${e.getMessage()} for ${request.getRequestURI()}")
        }
        sendError(response, code, e.getMessage(), e)
    }
    
    static private int mapError(Exception e) {
        switch(e) {
            case BadRequestException:
            case ModelValidationException:
            case LinkValidationException:
            case PostgreSQLComponent.ConflictingHoldException:    
                return HttpServletResponse.SC_BAD_REQUEST
            
            case NotFoundException:
                return HttpServletResponse.SC_NOT_FOUND
            
            case UnsupportedContentTypeException:
                return HttpServletResponse.SC_NOT_ACCEPTABLE
            
            case WhelkRuntimeException:
                return HttpServletResponse.SC_INTERNAL_SERVER_ERROR

            case StorageCreateFailedException:
                return HttpServletResponse.SC_CONFLICT
          
            case OtherStatusException:
                return ((OtherStatusException) e).code

            default: 
                return HttpServletResponse.SC_INTERNAL_SERVER_ERROR
        }
    }
    
    static class NotFoundException extends NoStackTraceException {
        NotFoundException(String msg) {
            super(msg)
        }
    }

    static class OtherStatusException extends NoStackTraceException {
        int code
        OtherStatusException(String msg, int code, Throwable cause = null) {
            super(msg, cause)
            this.code = code
        }
    }

    /** "Don't use exceptions for flow control" in part comes from that exceptions in Java are
     * expensive to create because building the stack trace is expensive. But in the context of 
     * sending error responses in this API exceptions are pretty useful for flow control. 
     * This is a base class for stack trace-less exceptions for common error flows.
     */
    static class NoStackTraceException extends RuntimeException {
        protected NoStackTraceException(String msg) {
            super(msg, null, true, false)
        }
        
        protected NoStackTraceException(String msg, Throwable cause) {
            super(msg, cause, true, false)
        }
    }
}
