package se.kb.libris.whelks.plugin

import javax.script.*
import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.persistance.*

import org.json.simple.JSONObject
import org.codehaus.jackson.map.ObjectMapper

@Log
class MarcFieldLabelerIndexFormatConverter extends BasicPlugin implements IndexFormatConverter, WhelkAware {

    int order = 100
    Whelk whelk
    ObjectMapper mapper

    def oldfacit = [
        "bibid": ["001"],
        "f":     ["100.a","505.r","700.a"],
        "förf":  ["100.a","505.r","700.a"],
        "isbn":  ["020.az"],
        "issn":  ["022.amyz"],
        "t":     ["242.ab","245.ab","246.ab","247.ab","249.ab","740.anp"],
        "tit":   ["242.ab","245.ab","246.ab","247.ab","249.ab","740.anp"],
        "titel": ["242.ab","245.ab","246.ab","247.ab","249.ab","740.anp"]
        ]

    def facit = [
        "020":   ["a":"isbn", "z":"isbn"],
        "022":   ["a":"issn", "m":"issn", "y":"issn", "z":"issn"],
        "100":   ["a":"author"],
        "505":   ["r":"author"],
        "700":   ["a":"author"],
        "242":   ["a":"title", "b": "title"],
        "245":   ["a":"title", "b": "title"],
        "246":   ["a":"title", "b": "title"],
        "247":   ["a":"title", "b": "title"],
        "249":   ["a":"title", "b": "title"],
        "720":   ["a":"title", "n": "title", "p": "title"]
    ]


    MarcFieldLabelerIndexFormatConverter() {
        mapper = new ObjectMapper()
    }

    @Override
    List<Document> convert(Document doc) {
        return convert([doc])
    }

    @Override
    List<Document> convert(List<Document> docs) {
        log.debug("Converting ${docs.size} documents")
        def outdocs = []
        for (doc in docs) {
            def json = mapper.readValue(doc.dataAsString, Map)
            json.fields.each {
                it.each { field, data ->
                    //log.debug("Field: $field : $data")
                    if (facit.containsKey(field)) {
                       // log.debug("facit: " + facit[field])
                        facit[field].each { f, v ->
                            /*
                            log.debug("F: $f, V: $v")
                            log.debug "subfields: " + data["subfields"]
                            */
                            data["subfields"].each { pair ->
                                if (pair[f]) {
                                    if (!json.labels) {
                                        json["labels"] = [:]
                                    }
                                    if (!json.labels[v]) {
                                        json.labels[v] = []
                                    }
                                    if (v == "isbn") {
                                        pair[f] = pair[f].replaceAll(/\D/, "")
                                    }
                                    json.labels[v].add(pair[f])
                                    log.debug "Put "+ pair[f] + " in $v"
                                }
                            }
                        }
                    }
                }
            }
            log.debug("Final json: $json")
            outdocs << new BasicDocument(doc).withData(mapper.writeValueAsBytes(json))
        }
        return outdocs
    }
}

@Log
class PythonRunnerFormatConverter extends BasicPlugin implements FormatConverter, WhelkAware, JSONSerialisable, JSONInitialisable {

    final private ScriptEngine python 
    String scriptName
    def script
    Reader reader
    String id = "PythonRunnerFormatConverter"
    Map<String, Object> requirements = new HashMap<String, Object>()
    Whelk whelk

    PythonRunnerFormatConverter(Map req) {
        python = new ScriptEngineManager().getEngineByName("python")
        this.scriptName = req.get("script")
        this.requirements = req 
        readScript(req.get("script"))
    }

    private readScript(scriptName) {
        Reader r = null
        if (scriptName.startsWith("/")) {
            this.script = new File(scriptName).text
        } else {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(scriptName)
            this.script = is.text
        }
        if (!this.script) {
            throw new WhelkRuntimeException("Failed to read script.")
        }
        this.reader = new StringReader(this.script)
    }


    @Override
    public List<Document> convert(List<Document> docs) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Document> convert(Document doc) {
        if (python == null) {
            throw new WhelkRuntimeException("Unable to find script engine for python.")
        }
        try {
            log.debug("Converter executing script "+ this.scriptName)
            requirements.each {
                log.debug("Feeding python with ${it.value} (${it.key})")
                python.put(it.key, it.value)
            }
            println "1: " + System.currentTimeMillis()
            Reader r = new StringReader(this.script)
            println "2: " + System.currentTimeMillis()
            //log.debug("Feeding python with $doc.identifier (document)")
            python.put("document", doc)
            println "3. " + System.currentTimeMillis()
            python.eval(r)
            println "4: " + System.currentTimeMillis()
            /*
            Object result = python.get("result")
            if (result != null) {
                return doc.withData(((String)result).getBytes())
            } else {
                log.debug("Python has handled everything for us. Now return null.")
                return null
            }
            */
            reader.reset()
            log.debug("Done ...")
        } catch (ScriptException se) {
            log.error("Script failed: " + se.getMessage(), se)
            return null
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)
        }
        return [doc]
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        log.debug("Initializing pythonrunner.")
        log.debug("Scriptname is " + obj.get("scriptName"))
        try {
            this.scriptName = obj.get("scriptName").toString()
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)

        }
        return this

    }

    @Override
    public JSONObject serialize() {
        JSONObject _converter = new JSONObject()
        _converter.put("_classname", this.getClass().getName())
        _converter.put("scriptName", this.scriptName)
                
        return _converter
    }

    protected void listAvailableEngines() {
        try {
            ScriptEngineManager mgr = new ScriptEngineManager()
            List<ScriptEngineFactory> factories = mgr.getEngineFactories()
            for (ScriptEngineFactory factory: factories) {
                System.out.println("ScriptEngineFactory Info")
                String engName = factory.getEngineName()
                String engVersion = factory.getEngineVersion()
                String langName = factory.getLanguageName()
                String langVersion = factory.getLanguageVersion()
                System.out.printf("\tScript Engine: %s (%s)\n",
                        engName, engVersion)
                List<String> engNames = factory.getNames()
                for(String name: engNames) {
                    System.out.printf("\tEngine Alias: %s\n", name)
                }
                System.out.printf("\tLanguage: %s (%s)\n",
                        langName, langVersion)
            }
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)
        }
    }

    static void main(args) {
        def r = ["script": "test.py"]
        def p = new PythonRunnerFormatConverter(r)
        p.listAvailableEngines()
        p.convert(null)
    }

}
