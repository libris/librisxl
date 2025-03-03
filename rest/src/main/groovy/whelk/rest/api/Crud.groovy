package whelk.rest.api

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Log4j2 as Log
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
import whelk.exception.StaleUpdateException
import whelk.exception.UnexpectedHttpStatusException
import whelk.exception.WhelkRuntimeException
import whelk.history.History
import whelk.rest.api.CrudGetRequest.Lens
import whelk.rest.security.AccessControl
import whelk.util.WhelkFactory
import whelk.util.http.BadRequestException
import whelk.util.http.HttpTools
import whelk.util.http.MimeTypes
import whelk.util.http.NotFoundException
import whelk.util.http.OtherStatusException
import whelk.util.http.RedirectException

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.lang.management.ManagementFactory


import static whelk.rest.api.CrudUtils.ETag
import static whelk.util.http.HttpTools.getBaseUri
import static whelk.util.http.HttpTools.sendResponse
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

    static final RestMetrics metrics = new RestMetrics()

    Whelk whelk

    JsonLdValidator validator
    TargetVocabMapper targetVocabMapper

    AccessControl accessControl = new AccessControl()
    ConverterUtils converterUtils

    SiteSearch siteSearch

    Map<String, Tuple2<Document, String>> cachedFetches = [:]

    Crud() {
        // Do nothing - only here for Tomcat to have something to call
    }

    Crud(Whelk whelk) {
        this.whelk = whelk
    }

    JsonLd getJsonld() {
        return whelk.jsonld
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }

        siteSearch = new SiteSearch(whelk)
        validator = JsonLdValidator.from(jsonld)
        converterUtils = new ConverterUtils(whelk)

        cacheFetchedResource(whelk.systemContextUri)
        cacheFetchedResource(whelk.vocabUri)
        cacheFetchedResource(whelk.vocabDisplayUri)

        Tuple2<Document, String> docAndLoc = cachedFetches[whelk.systemContextUri]
        Document contextDoc = docAndLoc.v1
        if (contextDoc) {
            targetVocabMapper = new TargetVocabMapper(jsonld, contextDoc.data)
        }

    }

    protected void cacheFetchedResource(String resourceUri) {
        cachedFetches[resourceUri] = getDocumentFromStorage(resourceUri, null)
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request for ${request.pathInfo}")
        try {
            doGet2(request, response)
        } catch (Exception e) {
            // Don't log a full callstack! This happens all the time when a user drops its connection
            // without having first received what it asked for (typically a slow query).
            log.info("Attempting to send error response, after catching: ${e.toString()}")
            sendError(request, response, e)
        } finally {
            log.debug("Sending GET response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doGet2(HttpServletRequest request, HttpServletResponse response) {
        RestMetrics.Measurement measurement = null
        try {
            if (request.pathInfo == "/") {
                measurement = metrics.measure('INDEX')
                displayInfo(response)
            } else if (siteSearch.isSearchResource(request.pathInfo)) {
                measurement = metrics.measure('FIND')
                handleQuery(request, response)
            } else {
                measurement = metrics.measure('GET')
                handleGetRequest(CrudGetRequest.parse(request), response)
            }
        } finally {
            if (measurement != null) {
                measurement.complete()
            }
        }
    }

    void displayInfo(HttpServletResponse response) {
        Map info = siteSearch.appsIndex[whelk.applicationId]
        sendResponse(response, mapper.writeValueAsString(info), "application/json")
    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String baseUri = getBaseUri(request)

        try {
            Map results = siteSearch.findData(queryParameters, baseUri, request.pathInfo)
            String uri = request.getRequestURI()
            Map contextData = jsonld.context
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
            log.info("Invalid query: ${queryParameters}")
            throw new BadRequestException("Invalid query, please check the documentation. ${e.getMessage()}")
        } catch (RedirectException e) {
            sendRedirect(request, response, e.getMessage())
        }
    }

    void handleGetRequest(CrudGetRequest request,
                          HttpServletResponse response) {
        Tuple2<Document, String> docAndLocation

        if (request.getId() in cachedFetches) {
            docAndLocation = cachedFetches[request.getId()]
        } else {
            docAndLocation = getDocumentFromStorage(
                    request.getId(), request.getVersion().orElse(null))
        }

        Document doc = docAndLocation.v1
        String loc = docAndLocation.v2

        if (!doc && !loc) {
            sendNotFound(request.getHttpServletRequest(), response)
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
                    doc.applyInverses(jsonld)
                }
            } else {
                eTag = ETag.plain(doc.getChecksum(jsonld))
            }

            if (request.getIfNoneMatch().map(eTag.&isNotModified).orElse(false)) {
                sendNotModified(response, eTag)
                return
            }

            String profileId = request.getProfile().orElse(whelk.systemContextUri)
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
        HttpTools.sendError(response, HttpServletResponse.SC_NOT_MODIFIED, "Document has not been modified.")
    }

    private static void sendNotFound(HttpServletRequest request, HttpServletResponse response) {
        metrics.failedRequests.labels(request.getMethod(), HttpServletResponse.SC_NOT_FOUND.toString()).inc()
        HttpTools.sendError(response, HttpServletResponse.SC_NOT_FOUND, "Document not found.")
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

        Object contextData = jsonld.context
        if (profileId != whelk.systemContextUri) {
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
        return JsonLd.frame(document.getCompleteId(), document.data, 3)
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
                return jsonld.toCard(framedThing)
            case Lens.CHIP:
                return (Map) jsonld.toChip(framedThing)
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
            // it has been merged with another doc - main id is in sameAs of remaining doc
            // TODO this could/should be implemented as a replacedBy inside the tombstone?
            if (doc.deleted && !JsonLd.looksLikeIri(id)) {
                String iri = Document.BASE_URI.toString() + id + Document.HASH_IT
                String location = whelk.storage.getMainId(iri)
                if (location) {
                    return new Tuple2(null, location)
                }
            }

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
        } else if (contentType == MimeTypes.JSONLD && responseBody instanceof Map) {
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
        RestMetrics.Measurement measurement = metrics.measure("POST")
        log.debug("Handling POST request for ${request.pathInfo}")

        try {
            doPost2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            measurement.complete()
            log.info("Sending POST response with status " +
                     "${response.getStatus()} for ${request.pathInfo}")
        }
    }

    void doPost2(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo != "/" && request.pathInfo != "/data") {
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
        RestMetrics.Measurement measurement = metrics.measure("PUT")
        log.debug("Handling PUT request for ${request.pathInfo}")

        try {
            doPut2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            measurement.complete()
            log.info("Sending PUT response with status " +
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
            throw new NotFoundException("Document not found.")
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
                        log.info("Refused API update of document due to changed 'collection'")
                        throw new BadRequestException("Cannot change legacy collection for document. Legacy collection is mapped from entity @type.")
                    }

                    ETag ifMatch = Optional
                            .ofNullable(request.getHeader("If-Match"))
                            .map(ETag.&parse)
                            .orElseThrow({ -> new BadRequestException("Missing If-Match header in update") })
                    
                    log.info("If-Match: ${ifMatch}")
                    whelk.storeAtomicUpdate(doc, false, true, "xl", activeSigel, ifMatch.documentCheckSum())
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
        RestMetrics.Measurement measurement = metrics.measure("DELETE")
        log.debug("Handling DELETE request for ${request.pathInfo}")

        try {
            doDelete2(request, response)
        } catch (Exception e) {
            sendError(request, response, e)
        } finally {
            measurement.complete()
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
        } else {
            log.debug("Removing resource at ${doc.getShortId()}")
            String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER)
            whelk.remove(doc.getShortId(), "xl", activeSigel)
            response.setStatus(HttpServletResponse.SC_NO_CONTENT)
        }
    }

    static void sendError(HttpServletRequest request, HttpServletResponse response, Exception e) {
        int code = HttpTools.mapError(e)
        metrics.failedRequests.labels(request.getMethod(), code.toString()).inc()
        if (log.isDebugEnabled()) {
            log.debug("Sending error $code : ${e.getMessage()} for ${request.getRequestURI()}")
        }
        HttpTools.sendError(response, code, e.getMessage(), e)
    }


}
