package whelk.rest.api

import groovy.util.logging.Slf4j as Log
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
import whelk.exception.WhelkRuntimeException
import whelk.filter.JsonLdLinkExpander

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
class Rest extends HttpServlet {

    MimetypesFileTypeMap mimeTypes = new MimetypesFileTypeMap()
    final static Map contextHeaders = [
            "bib": "/sys/context/lib.jsonld",
            "auth": "/sys/context/lib.jsonld",
            "hold": "/sys/context/lib.jsonld"
    ]
    Whelk whelk
    PicoContainer pico

    Rest() {
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
                document = convertDocumentToAcceptedMediaType(document, path, request.getHeader("accept"))
            }

            if (document && (!document.isDeleted() || mode== HttpTools.DisplayMode.META)) {

                if (mode == HttpTools.DisplayMode.META) {
                    def versions = whelk.storage.loadAllVersions(document.identifier)
                    if (versions) {
                        document.manifest.versions = versions
                    }
                    sendResponse(response, document.getManifestAsJson(), "application/json")
                } else {
                    String ctheader = contextHeaders.get(path.split("/")[1])
                    if (ctheader)
                        response.setHeader("Link", "<$ctheader>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"")
                    }
                    response.setHeader("ETag", document.modified as String)
                    String contentType = getMajorContentType(document.contentType)
                    if (path in contextHeaders.collect { it.value })  {
                        log.debug("request is for context file. Must serve original content-type ($contentType).")
                        contentType = document.contentType
                    }
                    if (flat) {
                        sendResponse(response, JsonLd.flatten(document.data), contentType)
                    } else {
                        log.info("Framing ${document.identifier} ...")
                        sendResponse(response, JsonLd.frame(document.identifier, document.data), contentType)
                    }

            } else {
                log.debug("Failed to find a document with URI $path")
                response.sendError(response.SC_NOT_FOUND)
            }
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

    Document convertDocumentToAcceptedMediaType(Document document, String path, String acceptHeader) {

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
            def fc = plugins.find { it instanceof FormatConverter && accepting.contains(it.resultContentType) && it.requiredContentType == document.contentType }
            if (fc) {
                log.debug("Found formatconverter for ${fc.resultContentType}")
                document = fc.convert(document)
                if (extensionContentType) {
                    response.setHeader("Content-Location", path)
                }
            } else {
                document = null
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
    }

    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
    }

    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
    }



}
