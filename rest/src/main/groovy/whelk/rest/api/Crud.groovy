package whelk.rest.api

import groovy.util.logging.Slf4j as Log
import org.apache.http.entity.ContentType
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.PicoContainer
import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.IdGenerator
import whelk.component.ElasticSearch
import whelk.component.StorageType
import whelk.converter.FormatConverter
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.exception.StorageCreateFailedException
import whelk.exception.WhelkAddException
import whelk.exception.WhelkRuntimeException
import whelk.rest.security.AccessControl
import whelk.util.PropertyLoader

import javax.activation.MimetypesFileTypeMap
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static HttpTools.sendResponse
import static whelk.rest.api.HttpTools.getMajorContentType

/**
 * Handles all GET/PUT/POST/DELETE requests against the backend.
 *
 * Created by markus on 2015-10-09.
 */
@Log
class Crud extends HttpServlet {

    final static String SAMEAS_NAMESPACE = "http://www.w3.org/2002/07/owl#sameAs"
    final static String DOCBASE_URI = "http://libris.kb.se/"

    MimetypesFileTypeMap mimeTypes = new MimetypesFileTypeMap()

    final static Map contextHeaders = [
            "bib": "/sys/context/lib.jsonld",
            "auth": "/sys/context/lib.jsonld",
            "hold": "/sys/context/lib.jsonld"
    ]
    Whelk whelk
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
        pico.addComponent(ISXNTool.class)

        //pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ApixClientCamel.class)
        //pico.addComponent(Characteristics.CACHE).addComponent(JsonLdLinkExpander.class)

        pico.start()
    }

    @Override
    void init() {
        whelk = pico.getComponent(Whelk.class)
    }

    StorageType autoDetectQueryMode(Map queries) {
        boolean probablyMarcQuery = false
        for (entry in queries) {
            if (entry.key ==~ /\d{3}\.{0,1}\w{0,1}/) {
                probablyMarcQuery = true
            } else if (!entry.key.startsWith("_")) {
                probablyMarcQuery = false
            }
        }
        return probablyMarcQuery ? StorageType.MARC21_JSON : StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS
    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response, String dataset) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String callback = queryParameters.remove("callback")
        Map results = null
        if (queryParameters.containsKey("q")) {
            // If general q-parameter chosen, use elastic for query
            def dslQuery = ElasticSearch.createJsonDsl(queryParameters)
            if (whelk.elastic) {
                results = whelk.elastic.query(dslQuery, dataset)
            } else {
                log.error("Attempted elastic query, but whelk has no elastic component configured.")
                response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "Attempted to use elastic for query, but no elastic component is configured.")
                return
            }
        } else {
            results = whelk.storage.query(queryParameters, dataset, autoDetectQueryMode(queryParameters))
        }

        def jsonResult = (callback ? callback + "(" : "") + mapper.writeValueAsString(results) + (callback ? ");" : "")

        sendResponse(response, jsonResult, "application/json")
    }

    void displayInfo(response) {
        def info = [:]
        info["system"] = "LIBRISXL"
        info["version"] = whelk.storage.loadSettings("system").get("version")
        info["format"] = "linked-data-api"
        info["collections"] = whelk.storage.loadCollections()
        sendResponse(response, mapper.writeValueAsString(info), "application/json")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        if (request.pathInfo.endsWith("/")) {
            if (request.pathInfo == "/") {
                displayInfo(response)
            } else {
                handleQuery(request, response, request.pathInfo.replaceAll("/", ""))
            }
            return
        }

        try {
            def (path, mode) = determineDisplayMode(request.pathInfo)
            String version = request.getParameter("version")
            boolean flat = request.getParameter("flat") == "true"

            Document document
            if (version) {
                document = whelk.storage.load(path, version)
            } else {
                Location location = whelk.storage.locate(path, false)
                document = location?.document
                if (!document && location?.uri) {
                    log.debug("Redirecting to document location: ${location.uri}")
                    sendRedirect(request, response, location)
                    return
                }
            }

            if (HttpTools.DisplayMode.DOCUMENT == mode) {
                document = convertDocumentToAcceptedMediaType(document, path, request.getHeader("accept"), response)
            }

            if (document && (!document.isDeleted() || mode == HttpTools.DisplayMode.META)) {

                if (mode == HttpTools.DisplayMode.META) {
                    def versions = whelk.storage.loadAllVersions(document.identifier)
                    if (versions) {
                        document.manifest.versions = versions.collect { ["version":it.version, "modified": it.modified as String, "checksum":it.manifest.checksum] }
                    }
                    sendResponse(response, document.getManifestAsJson(), "application/json")
                } else {
                    String ctheader = contextHeaders.get(path.split("/")[1])
                    if (ctheader) {
                        response.setHeader("Link", "<$ctheader>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"")
                    }
                }
                response.setHeader("ETag", document.modified.getTime() as String)
                String contentType = getMajorContentType(document.contentType)
                if (path in contextHeaders.collect { it.value }) {
                    log.debug("request is for context file. Must serve original content-type ($contentType).")
                    contentType = document.contentType
                }
                if (document.isJsonLd()) {
                    sendResponse(response, (flat ? JsonLd.flatten(document.data) : JsonLd.frame(document.identifier, document.data)), contentType)
                } else if (document.isJson()) {
                    sendResponse(response, document.data, contentType)
                } else {
                    sendResponse(response, document.data.get(Document.NON_JSON_CONTENT_KEY) ?: document.data, contentType) // For non json data, the convention is to keep the data in "content"
                }
            } else {
                log.debug("Failed to find a document with URI $path")
                response.sendError(response.SC_NOT_FOUND)
            }
        } catch (UnsupportedContentTypeException ucte) {
            response.sendError(response.SC_NOT_ACCEPTABLE, ucte.message)
        } catch (WhelkRuntimeException wrte) {
            response.sendError(response.SC_INTERNAL_SERVER_ERROR, wrte.message)
        }
    }

    def determineDisplayMode(path) {
        if (path.endsWith("/meta")) {
            return [path[0 .. -6], HttpTools.DisplayMode.META]
        }
        if (path.endsWith("/_raw")) {
            return [path[0 .. -6], HttpTools.DisplayMode.RAW]
        }
        return [path, HttpTools.DisplayMode.DOCUMENT]
    }

    Document convertDocumentToAcceptedMediaType(Document document, String path, String acceptHeader, HttpServletResponse response) {

        List<String> accepting = acceptHeader?.split(",").collect {
            int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
            it.substring(0,last)
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
            FormatConverter fc = pico.getComponents(FormatConverter.class).find { accepting.contains(it.resultContentType) && it.requiredContentType == document.contentType }
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

    void sendRedirect(HttpServletRequest request, HttpServletResponse response, Location location) {
        def locationRef = request.getScheme() + "://" + request.getServerName() + (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") + request.getContextPath()
        response.setHeader("Location", locationRef + location.uri.toString())
        sendResponse(response, new byte[0], null, location.responseCode)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        doPostOrPut(request, response)
    }

    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        doPostOrPut(request, response)
    }

    void doPostOrPut(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Executing ${request.getMethod()}-request")
        String dataset = getDatasetBasedOnPath(request.pathInfo)
        byte[] data = request.getInputStream().getBytes()
        try {
            Document doc = createDocumentIfOkToSave(data, dataset, request, response)
            if (doc) {
                if (doc.contentType == "application/ld+json") {
                    log.debug("Flattening ${doc.id}")
                    doc.data = JsonLd.flatten(doc.data)
                }
                log.debug("Saving document (${doc.identifier})")
                doc = whelk.store(doc, (request.getMethod() == "PUT"))

                sendDocumentSavedResponse(response, getResponseUrl(request, doc.identifier), doc.modified.getTime() as String)
            }
        } catch (StorageCreateFailedException scfe) {
            log.warn("Already have document with id ${scfe.duplicateId}")
            response.sendError(HttpServletResponse.SC_CONFLICT, "Document with id \"${scfe.duplicateId}\" already exists.")
        } catch (WhelkAddException wae) {
            log.warn("Whelk failed to store document: ${wae.message}")
            response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE , wae.message)
        } catch (Exception e) {
            log.error("Operation failed", e)
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.message)
        }
    }

    Document createDocumentIfOkToSave(byte[] data, String dataset, HttpServletRequest request, HttpServletResponse response) {
        log.debug("dataset is $dataset")
        if (data.length == 0) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No data received")
            return null
        }
        String cType = ContentType.parse(request.getContentType()).getMimeType()
        if (cType == "application/x-www-form-urlencoded") {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Content-Type application/x-www-form-urlencoded is not supported")
            return null
        }

        Map dataMap
        if (Document.isJson(cType)) {
            dataMap = mapper.readValue(data, Map)
        } else {
            dataMap = [(Document.NON_JSON_CONTENT_KEY): new String(data)]
        }

        String identifier = null
        if (request.method == "POST") {
            int pathsize = request.pathInfo.split("/").size()
            log.debug("pathsize: $pathsize")
            if (pathsize != 0 && pathsize != 2) {
                response.sendError(response.SC_BAD_REQUEST, "POST only allowed to root or collection")
                return null
            }
            if (Document.isJsonLd(cType)) {
                identifier = JsonLd.findIdentifier(dataMap)
            }
        }
        if (request.method == "PUT") {
            if (request.pathInfo == "/") {
                response.sendError(response.SC_BAD_REQUEST, "PUT not allowed to ROOT")
                return null
            }
            identifier = request.pathInfo.substring(1)
        }

        Document existingDoc

        if (identifier) {
            String foundIdentifier = JsonLd.findIdentifier(dataMap)
            if (foundIdentifier && foundIdentifier != identifier) {
                response.sendError(response.SC_CONFLICT, "The supplied data contains an @id ($foundIdentifier) which differs from the URI in the PUT request ($identifier)")
                return null
            }
            existingDoc = whelk.storage.locate(identifier, true)?.document
        }

        if (existingDoc) {
            if (request.method == "POST") {
                response.sendError(HttpServletResponse.SC_CONFLICT, "Document with identifier ${identifier} already exists. Use PUT if you want to update.")
                return null
            }
            if (request.getHeader("If-Match") && existingDoc.modified as String != request.getHeader("If-Match")) {
                log.debug("Document with identifier ${existingDoc.identifier} already exists.")
                response.sendError(response.SC_PRECONDITION_FAILED, "The resource has been updated by someone else. Please refetch.")
                return null
            }
            log.debug("Identifier was ${identifier}. Setting to ${existingDoc.id}")
            identifier = existingDoc.id
        }


        if (!identifier) {
            identifier = mintIdentifier(dataMap)
        }

        Document doc = new Document(identifier, dataMap, existingDoc?.manifest)

        log.debug("dataset is now $dataset")

        doc = doc.inCollection(dataset)
                .withContentType(ContentType.parse(request.getContentType()).getMimeType())
                .withIdentifier(identifier)
                .withDeleted(false)

        getAlternateIdentifiersFromLinkHeaders(request).each {
            doc.addIdentifier(it);
        }

        log.debug("Checking permissions for ${doc}")
        if (!hasPermission(request.getAttribute("user"), doc, existingDoc)) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            return null
        }

        log.debug("All checks passed")

        return doc
    }

    // TODO: fix this for new URI:s
    String getResponseUrl(HttpServletRequest request, String identifier) {
        StringBuffer locationRef = request.getRequestURL()
        while (locationRef[-1] == '/') {
            locationRef.deleteCharAt(locationRef.length() - 1)
        }

        /*
        if (dataset && locationRef.toString().endsWith(dataset)) {
            int endPos = locationRef.length()
            int startPos = endPos - dataset.length() - 1
            locationRef.delete(startPos, endPos)
        }
        */
        if (!locationRef.toString().endsWith(identifier)) {
            locationRef.append("/")
            locationRef.append(identifier)
        }

        return locationRef.toString()
    }

    void sendDocumentSavedResponse(HttpServletResponse response, String locationRef, String etag) {
        log.debug("Setting header Location: $locationRef")
        response.setHeader("Location", locationRef)
        response.setHeader("ETag", etag as String)
        response.setStatus(HttpServletResponse.SC_CREATED)
    }


    boolean hasPermission(info, newdoc, olddoc) {
        if (info) {
            log.debug("User is: $info")
            if (info.user == "SYSTEM") {
                return true
            }
            return accessControl.checkDocument(newdoc, olddoc, info)
        }
        log.info("No user information received, denying request.")
        return false
    }

    static String getDatasetBasedOnPath(path) {
        String ds = ""
        def elements = path.split("/")
        int i = 1
        while (elements.size() > i && ds.length() == 0) {
            ds = elements[i++]
        }
        log.trace("Estimated dataset: $ds")
        return ds
    }

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
        try {
            String id = request.pathInfo.substring(1)
            def doc = whelk.storage.load(id)
            if (doc && !hasPermission(request.getAttribute("user"), doc, null)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            } else {
                log.debug("Removing resource at ${id}")
                whelk.remove(id)
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)

            }
        } catch (WhelkRuntimeException wre) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
        }

    }



}
