package whelk

import groovy.util.logging.Slf4j as Log

import java.net.URISyntaxException
import java.util.regex.*
import java.util.UUID
import java.util.concurrent.*
import javax.servlet.http.*
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import java.lang.management.*


import whelk.api.*
import whelk.camel.*
import whelk.camel.route.*
import whelk.component.*
import whelk.exception.*
import whelk.plugin.*
import whelk.result.*

import whelk.util.Tools

import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.map.ObjectMapper

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder

import org.apache.activemq.camel.component.ActiveMQComponent

@Log
class StandardWhelk extends HttpServlet implements Whelk {

    String id
    List<Plugin> plugins = new ArrayList<Plugin>()
    List<Storage> storages = new ArrayList<Storage>()
    Map<Pattern, API> apis = new LinkedHashMap<Pattern, API>()

    final Map locationConfig = ["preCursor": "/resource"]

    Index index
    GraphStore graphStore

    // Set by configuration
    Map global = [:]
    URI docBaseUri
    Map documentDataToMetaMapping = null

    final static ObjectMapper mapper = new ObjectMapper()

    final static String DEFAULT_WHELK_CONFIG_FILENAME = "whelk.json"
    final static String DEFAULT_PLUGIN_CONFIG_FILENAME = "plugins.json"

    // Set by init()-method
    CamelContext camelContext = null

    private ProducerTemplate producerTemplate

    static final DateTimeFormatter DT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.n]XXX")

    def timeZone = ZoneId.systemDefault()

    long MAX_MEMORY_THRESHOLD = 95 // In percent

    /*
     * Whelk methods
     *******************************/
    @groovy.transform.CompileStatic
    String add(Document doc, boolean minorUpdate = false) {
        log.debug("Add single document ${doc.identifier}")
        if (!doc.data || doc.data.length < 1) {
            throw new DocumentException(DocumentException.EMPTY_DOCUMENT, "Tried to store empty document.")
        }
        def availableStorages = getStorages(doc.contentType)
        if (availableStorages.isEmpty()) {
            throw new WhelkAddException("No storages available for content-type ${doc.contentType}")
        }
        doc = prepareDocument(doc)
        boolean saved = false
        for (storage in availableStorages) {
            if (storage.eligibleForStoring(doc)) {
                doc = updateModified(doc)
                saved = (storage.store(doc) || saved)
                log.debug("Storage ${storage.id} result from save: $saved")
            } else if (log.isDebugEnabled()) {
                log.debug("Storage ${storage.id} didn't find document ${doc.identifier} eligible for storing.")
            }
        }
        if (saved) {
            if (!minorUpdate) {
                notifyCamel(doc, ADD_OPERATION, [:])
            } else if (log.isDebugEnabled()) {
                log.debug("Saved document silently, without notifying camel.")
            }
        }
        return doc.identifier
    }

    /**
     * Requires that all documents have an identifier.
     */
    @groovy.transform.CompileStatic
    void bulkAdd(final List<Document> docs, String contentType, boolean prepareDocuments = true) {
        checkAvailableMemory()
        log.debug("Bulk add ${docs.size()} documents")
        def suitableStorages = getStorages(contentType)
        if (suitableStorages.isEmpty()) { 
            log.debug("No storages found for $contentType.")
            return
        }
        if (prepareDocuments) {
            for (doc in docs) {
                doc = prepareDocument(doc)
            }
        }
        log.debug("Sending to storage(s)")
        for (storage in suitableStorages) {
            storage.bulkStore(docs)
        }
        notifyCamel(docs)
        log.debug("Bulk operation completed")
    }

    Document get(String identifier, String version=null, List contentTypes=[], boolean expandLinks = true) {
        Document doc = null
        for (contentType in contentTypes) {
            log.trace("Looking for $contentType storage.")
            def s = getStorage(contentType)
            if (s) {
                log.debug("Found $contentType storage ${s.id}.")
                doc = s.get(identifier, version)
                break
            }
        }
        // TODO: Check this
        if (!doc) {
            doc = storage.get(identifier, version)
        }

        return doc
    }

    Location locate(String uri) {
        log.debug("Locating $uri")
        if (uri) {
            def doc = get(uri)
            if (doc) {
                return new Location(doc)
            }

            String identifier = new URI(uri).getPath().toString()
            log.trace("Nothing found at identifier $identifier")

            if (locationConfig['preCursor'] && identifier.startsWith(locationConfig['preCursor'])) {
                identifier = identifier.substring(locationConfig['preCursor'].length())
                log.trace("New place to look: $identifier")
            }
            if (locationConfig['postCursor'] && identifier.endsWith(locationConfig['postCursor'])) {
                identifier = identifier.substring(0, identifier.length() - locationConfig['postCursor'].length())
                log.trace("New place to look: $identifier")
            }
            log.debug("Checking if new identifier (${identifier}) has something to get")
            if (get(identifier)) {
                return new Location().withURI(new URI(identifier)).withResponseCode(303)
            }

            log.debug("Check alternate identifiers.")
            doc = storage.getByAlternateIdentifier(uri)
            if (doc) {
                return new Location().withURI(new URI(doc.identifier)).withResponseCode(301)
            }

            if (index) {
                log.debug("Looking for identifiers in record.")
                // TODO: This query MUST be made against storage index. It will not be safe otherwise
                def query = new ElasticQuery(["terms":["sameAs.@id:"+identifier]])
                def result = index.query(query)
                if (result.numberOfHits > 1) {
                    log.error("Something is terribly wrong. Got too many hits for sameAs. Don't know how to handle it. Yet.")
                }
                if (result.numberOfHits == 1) {
                    log.trace("Results: ${result.toJson()}")
                    // TODO: Adapt to new search results.
                    def foundIdentifier = result.toMap(null, []).list[0].identifier
                    return new Location().withURI(foundIdentifier).withResponseCode(301)
                }
            }
        }

        return null
    }

    Map<String, String> getVersions(String identifier) {
        def docs = storage.getAllVersions(identifier)
        def versions = [:]
        for (d in docs) {
            versions[(d.version)] = d.checksum
        }
        return versions
    }

    void remove(String id, String dataset = null) {
        def doc= get(id)
        components.each {
            ((Component)it).remove(id)
        }
        if (doc?.dataset) {
            dataset = doc.dataset
        }
        log.debug("Sending DELETE operation to camel.")
        log.debug("document has identifier: ${doc?.identifier} with dataset ${dataset}")
        def extraInfo = [:]
        if (doc && !doc.deleted && doc instanceof JsonDocument) {
            // Temporary necessity to handle removal of librisxl-born documents from voyager
            extraInfo["controlNumber"] = doc.dataAsMap.get("controlNumber")
        }
        if (doc) {
            notifyCamel(doc, REMOVE_OPERATION, extraInfo)
        } else {
            notifyCamel(id, dataset, REMOVE_OPERATION, extraInfo)
        }
    }

    @Override
    SearchResult search(Query query) {
        return index?.query(query)
    }

    @Override
    InputStream sparql(String query) {
        return sparqlEndpoint?.sparql(query)
    }

    Document updateModified(Document doc, long mt = -1) {
        if (mt < 0) {
            mt = doc.updateModified()
        } else {
            doc.setModified(mt)
        }
        if (doc.contentType == "application/ld+json") {
            def map = doc.getDataAsMap()
            if (map.containsKey("modified")) {
                log.trace("Setting modified in document data.")
                def time = ZonedDateTime.ofInstant(new Date(mt).toInstant(), timeZone)
                def timestamp = time.format(DT_FORMAT)
                map.put("modified", timestamp)
                doc = doc.withData(map)
            }
        } else {
            log.info("Document with content-type ${doc.contentType} cannot have modified automatically updated in data.")
        }
        return doc
    }

    Document prepareDocument(Document doc) {
        doc = sanityCheck(doc)
        if (doc.contentType == "application/ld+json") {
            def map = doc.getDataAsMap()
            // TODO: Make this configurable, or move it to uriminter
            if (map.containsKey("about") && !map.get("about")?.containsKey("@id")) {
                map.get("about").put("@id", "/resource"+doc.identifier)
            }
            if (documentDataToMetaMapping) {
                def meta = doc.meta
                boolean modified = false
                documentDataToMetaMapping.each { dset, rules ->
                    if (doc.getDataset() == dset) {
                        rules.each { jsonldpath ->
                            try {
                                def value = Eval.x(map, "x.$jsonldpath")
                                if (value) {
                                    meta = Tools.insertAt(meta, jsonldpath, value)
                                    modified = true
                                }
                            } catch (MissingPropertyException mpe) {
                                log.trace("Unable to set meta property $jsonldpath : $mpe")
                            } catch (NullPointerException npe) {
                                log.warn("Failed to set $jsonldpath for $meta")
                            } catch (Exception e) {
                                log.error("Failed to extract meta info: $e", e)
                                throw e
                            }
                        }
                    }
                }
                if (modified) {
                    log.trace("Document meta is modified. Setting new values.")
                    doc.withMeta(meta)
                }
            }
            doc.withData(map)
        }
        return doc
    }


    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.withIdentifier(mintIdentifier(d))
            log.debug("Document was missing identifier. Setting identifier ${d.identifier}")
        }
        if (!d.data || d.data.length == 0) {
            log.error("No data in document.")
            throw new DocumentException("No data in document.")
        }

        // TODO: Self describing
        if (d.contentType == "application/ld+json") {
            // TODO: Make sure map serialization works.
            Map dataMap = d.dataAsMap
            if (!dataMap) {
                throw new DocumentException("Unable to deserialize data.")
            }
            if (dataMap.get("@id") != d.identifier) {
                log.trace("@id in data (${dataMap['@id']}) differs from document identifier ${d.identifier}. Correcting data.")
                dataMap.put("@id", d.identifier)
                d.withData(dataMap)
            }
        }
        return d
    }

    Iterable<Document> loadAll(Date since) { return loadAll(null, since, null)}

    Iterable<Document> loadAll(String dataset = null, Date since = null, String storageId = null) {
        def st
        if (storageId) {
            st = getStorages().find { it.id == storageId }
        } else {
            st = getStorage()
        }
        if (st) {
            log.debug("Loading "+(dataset ? dataset : "all")+" "+(since ?: "")+" from storage ${st.id}")
            return st.getAll(dataset, since)
        } else {
            throw new WhelkRuntimeException("Couldn't find storage. (storageId = $storageId)")
        }
    }

    Document createDocument(String contentType) {
        if (contentType ==~ /application\/(\w+\+)*json/ || contentType ==~ /application\/x-(\w+)-json/) {
            return new JsonDocument().withContentType(contentType)
        } else {
            return new DefaultDocument().withContentType(contentType)
        }
    }

    Document createDocumentFromJson(String json) {
        try {
            Document document = mapper.readValue(json, DefaultDocument)
            if (document.isJson()) {
                return new JsonDocument().fromDocument(document)
            }
            return document
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
    }


    @Override
    void flush() {
        log.debug("Flushing data.")
        // TODO: Implement storage and graphstore flush if necessary
        index?.flush()
    }

    void notifyCamel(List<Document> documents) {
        for (doc in documents) {
            notifyCamel(doc, BULK_ADD_OPERATION, [:])
        }
    }


    @Override
    void notifyCamel(String identifier, String dataset, String operation, Map extraInfo) {
        Exchange exchange = new DefaultExchange(getCamelContext())
        Message message = new DefaultMessage()
        message.setBody(identifier, String)
        if (extraInfo) {
            extraInfo.each { key, value ->
                message.setHeader("whelk:$key", value)
            }
        }
        message.setHeader("whelk:operation", operation)
        message.setHeader("document:identifier", identifier)
        message.setHeader("document:dataset", dataset)

        exchange.setIn(message)
        log.trace("Sending $operation message to camel regaring ${identifier}")
        sendCamelMessage(operation, exchange)
    }

    void notifyCamel(Document document, String operation, Map extraInfo) {
        Exchange exchange = new DefaultExchange(getCamelContext())
        Message message = new DefaultMessage()
        if (document.isJson()) {
            message.setBody(document.dataAsMap, Map)
        } else {
            message.setBody(document.data)
        }
        message.setHeader("document:metaentry", document.entry.inspect())
        // For conveniance
        message.setHeader("document:identifier", document.identifier)
        message.setHeader("document:dataset", document.dataset)
        message.setHeader("document:metaentry", document.metadataAsJson)
        if (extraInfo) {
            extraInfo.each { key, value ->
                message.setHeader("whelk:$key", value)
            }
        }
        message.setHeader("whelk:operation", operation)
        exchange.setIn(message)
        log.trace("Sending document in message to camel regaring ${document.identifier}")
        sendCamelMessage(operation, exchange)
    }

    void sendCamelMessage(String operation, Exchange exchange) {
        if (!producerTemplate) {
            producerTemplate = getCamelContext().createProducerTemplate();
        }

        producerTemplate.send(getCamelEndpoint(operation), exchange)

    }

    String getCamelEndpoint(String operation, withConfig = false) {
        def comp = global.get("CAMEL_MASTER_COMPONENT") ?: "seda"
        def prefix = global.get("CAMEL_CHANNEL_PREFIX") ?: ""
        def config = global.get("CAMEL_COMPONENT_CONFIG") ?: ""
        return comp+":"+(prefix? prefix + "." :"")+this.id+"."+operation + (withConfig && config ? "?"+config : "")
    }

    private checkAvailableMemory() {
        boolean logMessagePrinted = false
        MemoryMXBean mem=ManagementFactory.getMemoryMXBean();
        MemoryUsage heap=mem.getHeapMemoryUsage();

        long totalMemory=heap.getUsed();
        long maxMemory=heap.getMax();
        long used=(totalMemory * 100) / maxMemory;

        log.debug("totalMemory: $totalMemory")
        log.debug("maxMemory: $maxMemory")
        log.debug("used: $used")

        while (used > MAX_MEMORY_THRESHOLD) {
            if (!logMessagePrinted) {
                log.warn("Using more than $MAX_MEMORY_THRESHOLD percent of available memory ($totalMemory / $maxMemory). Blocking.")
                logMessagePrinted = true
            }
            totalMemory=heap.getUsed();
            maxMemory=heap.getMax();
            used=(totalMemory * 100) / maxMemory;
        }
    }


    String whelkStatus = "STARTING"

    /*
     * Servlet methods
     *******************************/
    void handleRequest(HttpServletRequest request, HttpServletResponse response) {
        String path = request.pathInfo
        API api = null
        List pathVars = []
        def whelkinfo = [:]
        whelkinfo["whelk"] = this.id
        whelkinfo["status"] = whelkStatus

        log.debug("Path is $path")
        try {
            if (request.method == "GET" && path == "/" && request.getServerPort() != 80) {
                def compManifest = [:]
                components.each {
                    def plList = []
                    for (pl in it.plugins) {
                        plList << ["id":pl.id, "class": pl.getClass().getName()]
                    }
                    compManifest[(it.id)] = ["class": it.getClass().getName(), "plugins": plList]
                }
                whelkinfo["components"] = compManifest
                printAvailableAPIs(response, whelkinfo)
            } else {
                (api, pathVars) = getAPIForPath(path)
                if (api) {
                    api.handle(request, response, pathVars)
                } else {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND, "No API found for $path")
                }
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
    String mintIdentifier(Document d) {
        String identifier
        for (minter in uriMinters) {
            identifier = minter.mint(d)?.toString()
        }
        if (!identifier) {
            try {
                if (d.entry.dataset) {
                    identifier = new URI("/" + d.entry.dataset + "/" + UUID.randomUUID()).toString();
                } else {
                    identifier = new URI("/"+ UUID.randomUUID()).toString();
                }
            } catch (URISyntaxException ex) {
                throw new WhelkRuntimeException("Could not mint URI", ex);
            }
        }
        return identifier
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

    @Override
    void init() {
        def ctxThread
        try {
            def (whelkConfig, pluginConfig) = loadConfig()
            setConfig(whelkConfig, pluginConfig)
            // Start all plugins
            for (component in this.components) {
                log.info("Starting component ${component.id}")
                component.start()
            }
            log.debug("Setting up and configuring Apache Camel")
            def whelkCamelMain = new WhelkCamelMain(
                    getCamelEndpoint(ADD_OPERATION, true),
                    getCamelEndpoint(BULK_ADD_OPERATION, true),
                    getCamelEndpoint(REMOVE_OPERATION, true)
                )



            if (global.containsKey('ACTIVEMQ_BROKER_URL')) {
                ActiveMQComponent amqreceive = ActiveMQComponent.activeMQComponent()
                amqreceive.setConnectionFactory(ActiveMQPooledConnectionFactory.createPooledConnectionFactory(global['ACTIVEMQ_BROKER_URL'], 10, 100))
                whelkCamelMain.addComponent("activemq", amqreceive)

                def sendQName = global.get("CAMEL_MASTER_COMPONENT")
                if (sendQName) {
                    ActiveMQComponent amqsend = ActiveMQComponent.activeMQComponent()
                    amqsend.setConnectionFactory(ActiveMQPooledConnectionFactory.createPooledConnectionFactory(global['ACTIVEMQ_BROKER_URL'], 10, 200))
                    whelkCamelMain.addComponent(sendQName, amqsend)
                }
            }

            for (route in plugins.findAll { it instanceof RouteBuilder }) {
                log.info("Adding route ${route.id}")
                whelkCamelMain.addRoutes(route)
            }

            camelContext = whelkCamelMain.camelContext

            ctxThread = Thread.start {
                log.debug("Starting Apache Camel")
                whelkCamelMain.run()
            }
            log.info("Whelk ${this.id} is now operational.")
        } catch (Exception e) {
            log.warn("Problems starting whelk ${this.id}.", e)
            if (ctxThread) {
                ctxThread.interrupt()
            }
            throw e
        }
        whelkStatus = "RUNNING"
    }

    @Override
    void destroy() {
        whelkStatus = "SHUTTING DOWN"
    }

    /*
     * Setup and configuration methods
     ************************************/
    @Override
    void addPlugin(Plugin plugin) {
        log.debug("[${this.id}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        if (plugin instanceof Storage) {
            this.storages.add(plugin)
        } else if (plugin instanceof Index) {
            if (index) {
                throw new PluginConfigurationException("Index ${index.id} already configured for whelk ${this.id}.")
            }
            this.index = plugin
        } else if (plugin instanceof GraphStore) {
            if (graphStore) {
                throw new PluginConfigurationException("GraphStore ${index.id} already configured for whelk ${this.id}.")
            }
            this.graphStore = plugin
        }
        // And always add to plugins
        this.plugins.add(plugin)
        log.debug("Running init on plugin ${plugin.id}")
        plugin.init()
    }

    private def loadConfig() {
        Map whelkConfig
        Map pluginConfig
        if (System.getProperty("whelk.config.uri") && System.getProperty("plugin.config.uri")) {
            log.info("Loading config specified by system properties.")
            def wcu = System.getProperty("whelk.config.uri")
            def pcu = System.getProperty("plugin.config.uri")
            URI whelkconfig = new URI(wcu)
            URI pluginconfig = new URI(pcu)
            log.info("Initializing whelk using definitions in $wcu, plugins in $pcu")
            try {
                whelkConfig = mapper.readValue(new URI(wcu).toURL().newInputStream(), Map)
                pluginConfig = mapper.readValue(new URI(pcu).toURL().newInputStream(), Map)
            } catch (Exception e) {
                throw new PluginConfigurationException("Failed to read configuration: ${e.message}", e)
            }
        } else {
            log.info("Loading config from classpath")
            try {
                whelkConfig = mapper.readValue(this.getClass().getClassLoader().getResourceAsStream(DEFAULT_WHELK_CONFIG_FILENAME), Map)
                pluginConfig = mapper.readValue(this.getClass().getClassLoader().getResourceAsStream(DEFAULT_PLUGIN_CONFIG_FILENAME), Map)
            } catch (Exception e) {
                throw new PluginConfigurationException("Failed to read configuration: ${e.message}", e)
            }
        }
        if (!whelkConfig || !pluginConfig) {
            throw new PluginConfigurationException("Could not find suitable config. Please set the 'whelk.config.uri' system property")
        }
        return [whelkConfig, pluginConfig]
    }

    private void setConfig(whelkConfig, pluginConfig) {
        def disabled = System.getProperty("disable.plugins", "").split(",")
        setId(whelkConfig["_id"])
        setDocBaseUri(whelkConfig["_docBaseUri"])
        documentDataToMetaMapping = whelkConfig["_docMetaMapping"]
        this.global = whelkConfig["_properties"].asImmutable()
        whelkConfig["_plugins"].each { key, value ->
            log.trace("key: $key, value: $value")
            if (!(key =~ /^_.+$/)) {
                log.trace("Found a property to set for ${this.id}: $key = $value")
                this."$key" = value
            } else if (value instanceof List) {
                log.info("Adding plugins from group $key")
                for (p in value) {
                    if (!disabled.contains(p)) {
                        def plugin = getPlugin(pluginConfig, p, this.id)
                        //log.info("Adding plugin ${plugin.id} to ${this.id}")
                        addPlugin(plugin)
                    } else {
                        log.info("Plugin \"${p}\" has been disabled because you said so.")
                    }
                }
            }
        }
        whelkConfig["_apis"].each { apiEntry ->
            apiEntry.each {
                log.debug("Found api: ${it.value}, should attach at ${it.key}")
                API api = getPlugin(pluginConfig, it.value, this.id)
                api.setWhelk(this)
                api.init()
                apis.put(Pattern.compile(it.key), api)
            }
        }
    }

    def translateParams(params, whelkname) {
        def plist = []
        if (params instanceof String) {
            for (param in params.split(",")) {
                param = param.trim()
                if (param == "_whelkname") {
                    plist << whelkname
                } else if (param.startsWith("_property:")) {
                    plist << global.get(param.substring(10))
                } else {
                    plist << param
                }
            }
        } else if (params instanceof Map) {
            params.each {
                if (it.value instanceof String && it.value.startsWith("_property")) {
                    params.put(it.key, global.get(it.value.substring(10)))
                }
            }
            plist << params
        } else {
            plist << params
        }
        return plist
    }

    protected Map<String,Plugin> availablePlugins = new HashMap<String,Plugin>()

    def getPlugin(pluginConfig, plugname, whelkname, pluginChain=[:]) {
        if (availablePlugins.containsKey(plugname)) {
            log.debug("Recycling plugin $plugname")
            return availablePlugins.get(plugname)
        }
        def plugin
        pluginConfig.each { label, meta ->
            if (label == plugname) {
                if (meta._params) {
                    log.trace("Plugin $label has parameters.")
                    def params = translateParams(meta._params, whelkname)
                    log.trace("Plugin parameter: ${params}")
                    def pclasses = params.collect { it.class }
                    try {
                        def c = Class.forName(meta._class).getConstructor(pclasses as Class[])
                        log.trace("c: $c")
                        plugin = c.newInstance(params as Object[])
                        log.trace("plugin: $plugin")
                    } catch (NoSuchMethodException nsme) {
                        log.trace("Constructor not found the easy way. Trying to find assignable class.")
                        for (cnstr in Class.forName(meta._class).getConstructors()) {
                            log.trace("Found constructor for ${meta._class}: $cnstr")
                            log.trace("Parameter types: " + cnstr.getParameterTypes())
                            boolean match = true
                            int i = 0
                            for (pt in cnstr.getParameterTypes()) {
                                log.trace("Loop parameter type: $pt")
                                log.trace("Check against: " + params[i])
                                if (!pt.isAssignableFrom(params[i++].getClass())) {
                                    match = false
                                }
                            }
                            if (match) {
                                plugin = cnstr.newInstance(params as Object[])
                                break;
                            }
                        }
                    }
                } else {
                    log.trace("Plugin $label has no parameters.")
                    //try singleton plugin
                    try {
                        log.trace("Trying getInstance()-method.")
                        plugin = Class.forName(meta._class).getDeclaredMethod("getInstance").invoke(null,null)
                    } catch (NoSuchMethodException nsme) {
                        log.trace("No getInstance()-method. Trying constructor.")
                        plugin = Class.forName(meta._class).newInstance()
                    }
                }
                assert plugin, "Failed to instantiate plugin: ${plugname} from class ${meta._class} with params ${meta._params}"
                if (meta._id) {
                    plugin.id = meta._id
                } else {
                    plugin.setId(label)
                }
                plugin.global = global
                log.trace("Looking for other properties to set on plugin \"${plugin.id}\".")
                meta.each { key, value ->
                    if (!(key =~ /^_.+$/)) {
                        log.trace("Found a property to set for ${plugin.id}: $key = $value")
                        try {
                            plugin."$key" = value
                        } catch (MissingPropertyException mpe) {
                            throw new PluginConfigurationException("Tried to set property $key in ${plugin.id} with value $value")
                        }
                    }
                }
                if (plugin instanceof WhelkAware) {
                    plugin.setWhelk(this)
                }
                pluginChain.put(plugname, plugin)
                if (meta._plugins) {
                    log.debug("Setting plugins for ${plugin.id}.")
                    for (plug in meta._plugins) {
                        if (availablePlugins.containsKey(plug)) {
                            log.debug("Using previously initiated plugin \"$plug\" for $plugname")
                            plugin.addPlugin(availablePlugins.get(plug))
                        } else if (pluginChain.containsKey(plug)) {
                            log.debug("Using plugin \"$plug\" from pluginChain for $plugname")
                            plugin.addPlugin(pluginChain.get(plug))
                        } else {
                            log.debug("Loading plugin $plug for ${plugin.id}")
                            def subplugin = getPlugin(pluginConfig, plug, whelkname, pluginChain)
                            plugin.addPlugin(subplugin)
                        }
                    }
                } else {
                    log.trace("Plugin ${plugin.id} has no _plugin parameter ($meta)")
                }
            }
        }
        if (!plugin) {
            throw new WhelkRuntimeException("For $whelkname; unable to instantiate plugin with name $plugname.")
        }
        plugin.setId(plugname)
        log.trace("Calling init on ${plugin} (${plugin.id})")
        plugin.init()
        log.debug("Stashing \"${plugin.id}\".")
        availablePlugins.put(plugname, plugin)
        return plugin
    }

    java.lang.reflect.Constructor findConstructor(Class c, Class p) {
        java.lang.reflect.Constructor constructor = null
        try {
            constructor = c.getConstructor(p)
            log.debug("Found constructor the classic way.")
        } catch (Exception e) {
            log.warn("Unable to get constructor for $p")
            constructor = null
        }
        if (!constructor) {
            for (cnstr in c.constructors) {
                if (cnstr.parameterTypes.length == 1 && cnstr.parameterTypes[0].isAssignableFrom(p)) {
                    log.debug("Found constructor for class $c with parameter $p : " + cnstr.paramterTypes()[0])
                    constructor = cnstr
                }
            }
        }
        return constructor
    }

    // Sugar methods
    List<Component> getComponents() { return plugins.findAll { it instanceof Component } }

    Storage getStorage() { return storages.get(0) }
    Storage getPrimaryStorage() { return storages.get(0) }
    List<Storage> getStorages(String rct) { return storages.findAll { it.handlesContent(rct) } }
    Storage getStorage(String rct) { return storages.find { it.handlesContent(rct) } }

    List<SparqlEndpoint> getSparqlEndpoints() { return plugins.findAll { it instanceof SparqlEndpoint } }
    SparqlEndpoint getSparqlEndpoint() { return plugins.find { it instanceof SparqlEndpoint } }
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
    List<Filter> getFilters() { return plugins.findAll { it instanceof Filter }}
    Importer getImporter(String id) { return plugins.find { it instanceof Importer && it.id == id } }
    List<Importer> getImporters() { return plugins.findAll { it instanceof Importer } }
    List<API> getAPIs() { return apis.values() as List}


    // Maintenance whelk methods
    String getId() { this.id }

    protected void setId(String id) {
        this.id = id
    }

    void setDocBaseUri(String uri) {
        this.docBaseUri = new URI(uri)
    }

}
