package whelk.rest

import groovy.util.logging.Slf4j as Log

import java.util.regex.*
import javax.servlet.http.*

import org.picocontainer.*
import org.picocontainer.containers.*
import org.codehaus.jackson.map.ObjectMapper

import whelk.exception.*
import whelk.component.*
import whelk.rest.api.DocumentAPI

@Log
class HttpWhelk extends HttpServlet {

    final static ObjectMapper mapper = new ObjectMapper()
    //Map<Pattern, API> apis = new LinkedHashMap<Pattern, API>()
    //Whelk whelk

    static final List<String> LOCAL_ADDRESSES = ['0:0:0:0:0:0:0:1', '192.168.', '10.']

    private PicoContainer pico

    /*
     * Servlet methods
     *******************************/
    void handleRequest(HttpServletRequest request, HttpServletResponse response) {
        String path = request.pathInfo
        //API api = null
        List pathVars = []
        def whelkinfo = [:]
        /*
        whelkinfo["whelk"] = whelk.id
        whelkinfo["status"] = whelk.state.get("status", "STARTING")
        */


        log.debug("Path is $path")
        try {
            if (request.method == "GET" && path == "/") {
                whelkinfo["version"] = whelk.loadVersionInfo()
                String remote_addr = request.getHeader("X-Forwarded-For") ?: request.getRemoteAddr()
                boolean local = false
                for (addr in LOCAL_ADDRESSES) {
                    if (remote_addr.startsWith(addr)) {
                        local = true
                    }
                }
                if (!local) {
                    log.info("Network address $remote_addr is not considered local. Not showing details.")
                }

                if (request.getServerPort() != 80 && local) {
                    def compManifest = [:]
                    whelk.components.each {
                        def plList = []
                        for (pl in it.plugins) {
                            def plStat = ["id":pl.id, "class": pl.getClass().getName()]
                            plStat.putAll(pl.getStatus())
                            plList << plStat
                        }
                        compManifest[(it.id)] = ["class": it.getClass().getName(), "plugins": plList]
                        compManifest[(it.id)].putAll(it.getStatus())
                    }
                    whelkinfo["components"] = compManifest
                }
                printAvailableAPIs(response, whelkinfo)
            } else {
                log.info("Accessing $path")
                def api = pico.getComponent(DocumentAPI.class)
                api.handle(request, response, [path])
                /*
                (api, pathVars) = getAPIForPath(path)
                if (api) {
                    api.handle(request, response, pathVars)
                } else {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND, "No API found for $path")
                }
                */
            }
        } catch (DownForMaintenanceException dfme) {
            whelkinfo["status"] = "UNAVAILABLE"
            whelkinfo["message"] = dfme.message
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE)
            response.setCharacterEncoding("UTF-8")
            response.setContentType("application/json")
            response.writer.write(mapper.writeValueAsString(whelkinfo))
            response.writer.flush()
        }
    }

    void printAvailableAPIs(HttpServletResponse response, Map whelkinfo) {
        whelkinfo["apis"] = apis.collect {
             [ "path" : it.key ,
                "id": it.value.id,
                "description" : it.value.description ]
        }
        response.setCharacterEncoding("UTF-8")
        response.setContentType("application/json")
        response.writer.write(mapper.writeValueAsString(whelkinfo))
        response.writer.flush()
    }

    /**
     * Redirect request to handleRequest()-method
     */
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }

    def getAPIForPath(String path) {
        for (entry in apis.entrySet()) {
            log.trace("${entry.key} (${entry.key.getClass().getName()}) = ${entry.value}")
            Matcher matcher = entry.key.matcher(path)
            if (matcher.matches()) {
                log.trace("$path matches ${entry.key}")
                int groupCount = matcher.groupCount()
                List pathVars = new ArrayList(groupCount)
                for (int i = 1; i <= groupCount; i++) {
                    pathVars.add(matcher.group(i))
                }
                log.debug("Matched API ${entry.value} with pathVars $pathVars")
                return [entry.value, pathVars]
            }
        }
        return [null, []]
    }

    @Override
    void init() {
        log.info("Starting httpwhelk.")

        Properties mainProps = new Properties()

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                                      ? new FileInputStream(System.getProperty("xl.secret.properties"))
                                      : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )

        Properties props = new Properties()

        props.load(secretsConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))

        pico.as(Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)

        pico.addComponent(DocumentAPI.class)

        pico.start()
        /*
        def postgres = pico.getComponent(PostgreSQLComponent.class)
        log.info("Postgres is ${postgres.getClass().getName()} with connect url: ${postgres.connectionUrl}")
        def doc = postgres.load("/bib/13531679")
        log.info("Loaded doc ${doc.identifier}")
        */
    }


}
