package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.net.URI
import java.net.URISyntaxException
import java.util.regex.*
import java.util.UUID
import java.util.concurrent.BlockingQueue
import javax.servlet.http.*

import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*

import se.kb.libris.conch.Tools

import org.codehaus.jackson.map.ObjectMapper

@Log
class StandardWhelk extends HttpServlet implements Whelk {

    String id
    List<Plugin> plugins = new ArrayList<Plugin>()
    List<Storage> storages = new ArrayList<Storage>()
    Map<Pattern, API> apis = new LinkedHashMap<Pattern, API>()
    List<LinkExpander> linkExpanders = new ArrayList<LinkExpander>()

    Index index
    GraphStore graphStore

    // Set by configuration
    Map global = [:]
    URI docBaseUri

    final static ObjectMapper mapper = new ObjectMapper()

    final static String DEFAULT_WHELK_CONFIG_FILENAME = "/whelk.json"
    final static String DEFAULT_PLUGIN_CONFIG_FILENAME = "/plugins.json"

    /*
     * Whelk methods
     *******************************/
    @Override
    URI add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata) {
        Document doc = new Document().withData(data).withEntry(entrydata).withMeta(metadata)
        return add(doc)
    }

    @Override
    @groovy.transform.CompileStatic
    URI add(Document doc) {
        log.debug("Add single document ${doc.identifier}")
        if (!doc.data || doc.data.length < 1) {
            throw new DocumentException(DocumentException.EMPTY_DOCUMENT, "Tried to store empty document.")
        }
        doc.updateTimestamp()
        def availableStorages = getStorages(doc.contentType)
        if (availableStorages.isEmpty()) {
            throw new WhelkAddException("No storages available for content-type ${doc.contentType}")
        }
        for (storage in availableStorages) {
            storage.add(doc)
        }
        return new URI(doc.identifier)
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    @groovy.transform.CompileStatic
    void bulkAdd(final List<Document> docs, String contentType) {
        log.debug("Bulk add ${docs.size()} document")
        for (doc in docs) {
            doc.updateTimestamp()
        }
        for (storage in getStorages(contentType)) {
            storage.bulkAdd(docs, contentType)
        }
    }

    @Override
    Document get(URI uri, version=null, List contentTypes=[], boolean expandLinks = true) {
        Document doc = null
        for (contentType in contentTypes) {
            log.trace("Looking for $contentType storage.")
            def s = getStorage(contentType)
            if (s) {
                log.debug("Found $contentType storage ${s.id}.")
                doc = s.get(uri, version)
                break
            }
        }
        // TODO: Check this
        if (!doc) {
            doc = storage.get(uri, version)
        }


        if (expandLinks) {
            LinkExpander le = getLinkExpanderFor(doc)
            if (le) {
                doc = le.expand(doc)
            }
        }

        return doc
    }

    @Override
    void remove(URI uri) {
        components.each {
            ((Component)it).remove(uri)
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

    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.withIdentifier(mintIdentifier(d))
            log.debug("Document was missing identifier. Setting identifier ${d.identifier}")
        }
        // TODO: Self describing
        if (d.contentType == "application/ld+json") {
            Map dataMap = d.dataAsMap
            if (dataMap.get("@id") != d.identifier) {
                dataMap.put("@id", d.identifier)
                d.withData(dataMap)
            }
        }
        d.timestamp = new Date().getTime()
        return d
    }

    @Override
    Iterable<Document> loadAll(Date since) { return loadAll(null, since, null)}

    @Override
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

    @Override
    void flush() {
        log.info("Flushing data.")
        // TODO: Implement storage and graphstore flush if necessary
        index?.flush()
    }

    /*
     * Servlet methods
     *******************************/
    void handleRequest(HttpServletRequest request, HttpServletResponse response) {
        String path = request.pathInfo
        API api = null
        List pathVars = []
        def whelkinfo = [:]
        whelkinfo["whelk"] = this.id
        whelkinfo["status"] = "Hardcoded at 'fine'. Should be more dynamic ..."

        log.debug("Path is $path")
        try {
            if (request.method == "GET" && path == "/") {
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
    URI mintIdentifier(Document d) {
        URI identifier
        for (minter in uriMinters) {
            identifier = minter.mint(d)
        }
        if (!identifier) {
            try {
                //identifier = new URI("/"+id.toString() +"/"+ UUID.randomUUID());
                // Temporary to enable kitin progress
                identifier = new URI("/bib/"+ UUID.randomUUID());
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
        try {
            def (whelkConfig, pluginConfig) = loadConfig()
            setConfig(whelkConfig, pluginConfig)
            // Start all plugins
            for (component in this.components) {
                log.info("Starting component ${component.id}")
                component.start()
            }
            log.info("Whelk ${this.id} is now operational.")
        } catch (Exception e) {
            log.warn("Problems starting whelk ${this.id}.", e)
            throw e
        }
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
        if (plugin instanceof Prawn) {
            prawnsActive = true
            log.debug("[${this.id}] Starting Prawn: ${plugin.id}")
            log.debug("Adding to queue ${plugin.trigger}")
            queues.get(plugin.trigger).add(plugin.getQueue())
            def t = new Thread(plugin)
            t.start()
            prawnThreads << t
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
        } else if (plugin instanceof LinkExpander) {
            this.linkExpanders.add(plugin)
        }
        // And always add to plugins
        this.plugins.add(plugin)
        plugin.init(this.id)
    }

    private def loadConfig() {
        Map whelkConfig
        Map pluginConfig
        if (System.getProperty("whelk.config.uri") && System.getProperty("plugin.config.uri")) {
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
            try {
                whelkConfig = mapper.readValue(this.getClass().getResourceAsStream(DEFAULT_WHELK_CONFIG_FILENAME), Map)
                pluginConfig = mapper.readValue(this.getClass().getResourceAsStream(DEFAULT_PLUGIN_CONFIG_FILENAME), Map)
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
                api.init(this.id)
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
                    for (plug in meta._plugins) {
                        if (availablePlugins.containsKey(plug)) {
                            log.debug("Using previously initiated plugin $plug for $plugname")
                            plugin.addPlugin(availablePlugins.get(plug))
                        } else if (pluginChain.containsKey(plug)) {
                            log.debug("Using plugin $plug from pluginChain for $plugname")
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
        plugin.init(this.id)
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
    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
    List<Filter> getFilters() { return plugins.findAll { it instanceof Filter }}
    Importer getImporter(String id) { return plugins.find { it instanceof Importer && it.id == id } }
    List<Importer> getImporters() { return plugins.findAll { it instanceof Importer } }
    LinkExpander getLinkExpanderFor(Document doc) { return linkExpanders.find { it.valid(doc) } }
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
