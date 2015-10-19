package whelk.rest.api

import groovy.util.logging.Slf4j as Log
import org.apache.http.entity.ContentType
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.PicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.FormatConverter
import whelk.converter.URIMinter
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.exception.DocumentException
import whelk.exception.WhelkAddException
import whelk.exception.WhelkRuntimeException
import whelk.filter.JsonLdLinkExpander
import whelk.rest.security.AccessControl

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

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                ? new FileInputStream(System.getProperty("xl.secret.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )

        Properties props = new Properties()

        props.load(secretsConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))

        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)
        pico.addComponent(JsonLD2MarcConverter.class)
        pico.addComponent(JsonLD2MarcXMLConverter.class)

        pico.addComponent(Whelk.class)

        pico.addComponent(Characteristics.CACHE).addComponent(JsonLdLinkExpander.class)

        pico.addComponent(DocumentAPI.class)
        pico.addComponent(ISXNTool.class)

        pico.start()
    }

    @Override
    void init() {
        whelk = pico.getComponent(Whelk.class)
    }


    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {

        /*
        if (request.pathInfo.endsWith("/")) {
            return handleQuery(request, response, request.pathInfo)
        }
        */

        try {
            def (path, mode) = determineDisplayMode(request.pathInfo)
            String version = request.getParameter("version")
            boolean flat = request.getParameter("flat") == "true"

            Document document
            if (version) {
                document = whelk.storage.load(path, version)
            } else {
                Location location = whelk.storage.locate(path)
                document = location?.document
                if (!document && location?.uri) {
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
                        document.manifest.versions = versions
                    }
                    sendResponse(response, document.getManifestAsJson(), "application/json")
                } else {
                    String ctheader = contextHeaders.get(path.split("/")[1])
                    if (ctheader) {
                        response.setHeader("Link", "<$ctheader>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"")
                    }
                }
                response.setHeader("ETag", document.modified as String)
                String contentType = getMajorContentType(document.contentType)
                if (path in contextHeaders.collect { it.value }) {
                    log.debug("request is for context file. Must serve original content-type ($contentType).")
                    contentType = document.contentType
                }
                if (document.isJson()) {
                    sendResponse(response, (flat ? JsonLd.flatten(document.data) : JsonLd.frame(document.identifier, document.data)), contentType)
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
                document = whelk.storage.load(path.substring(0, path.lastIndexOf(".")))
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
            //if (okToSave(data, dataset, request, response)) {
                if (doc.contentType == "application/ld+json") {
                    log.debug("Flattening ${doc.id}")
                    doc.data = JsonLd.flatten(doc.data)
                }
                log.debug("Saving document (${doc.identifier})")
                doc = whelk.store(doc)

                sendDocumentSavedResponse(response, getResponseUrl(request, doc.identifier, doc.dataset), doc.modified as String)
            }
        } catch (WhelkAddException wae) {
            log.warn("Whelk failed to store document: ${wae.message}")
            response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE , wae.message)
        } catch (Exception e) {
            log.error("Operation failed", e)
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.message)
        }
    }

    Document createDocumentIfOkToSave(byte[] data, String dataset, HttpServletRequest request, HttpServletResponse response) {
        if (data.length == 0) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No data received")
            return null
        }
        String cType = ContentType.parse(request.getContentType()).getMimeType()
        if (cType == "application/x-www-form-urlencoded") {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Content-Type application/x-www-form-urlencoded is not supported")
            return null
        }
        String identifier = request.pathInfo
        if (request.method == "POST") {
            int pathsize = request.pathInfo.split("/").size()
            log.debug("pathsize: $pathsize")
            if (pathsize != 0 && pathsize != 2) {
                response.sendError(response.SC_BAD_REQUEST, "POST only allowed to root or dataset")
                return null
            }
            identifier = mintIdentifier(dataset)
        }
        Document existingDoc = null
        if (request.method == "PUT") {
            if (request.pathInfo == "/") {
                response.sendError(response.SC_BAD_REQUEST, "PUT not allowed to ROOT")
                return null
            }
            existingDoc = whelk.storage.load(request.pathInfo)
            if (existingDoc) {
                if (request.getHeader("If-Match") && existingDoc.modified as String != request.getHeader("If-Match")) {
                    log.debug("Document with identifier ${existingDoc.identifier} already exists.")
                    response.sendError(response.SC_PRECONDITION_FAILED, "The resource has been updated by someone else. Please refetch.")
                    return null
                }
            }
        }

        Document doc
        if (Document.isJson(cType)) {
            doc = new Document(request.pathInfo, mapper.readValue(data, Map), existingDoc?.manifest)
        } else {
            doc = new Document(request.pathInfo, [(Document.NON_JSON_CONTENT_KEY): new String(data)], existingDoc?.manifest)
        }
        doc = doc.withDataset(dataset)
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



    String getResponseUrl(HttpServletRequest request, String identifier, String dataset) {
        StringBuffer locationRef = request.getRequestURL()
        while (locationRef[-1] == '/') {
            locationRef.deleteCharAt(locationRef.length() - 1)
        }

        if (dataset && locationRef.toString().endsWith(dataset)) {
            int endPos = locationRef.length()
            int startPos = endPos - dataset.length() - 1
            locationRef.delete(startPos, endPos)
        }

        if (!locationRef.toString().endsWith(identifier)) {
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

    String mintIdentifier(String dataset) {
        return new URI("/" + (dataset ? "${dataset}/" : "") + UUID.randomUUID()).toString();
    }


    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        try {
            def doc = whelk.storage.load(request.pathInfo)
            if (doc && !hasPermission(request.getAttribute("user"), doc, null)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            } else {
                log.debug("Removing resource at ${request.pathInfo}")
                whelk.remove(request.pathInfo, getDatasetBasedOnPath(request.pathInfo))
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)

            }
        } catch (WhelkRuntimeException wre) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
        }

    }



}
