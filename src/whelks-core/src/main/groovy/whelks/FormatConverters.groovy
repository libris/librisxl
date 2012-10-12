package se.kb.libris.whelks.plugin

import javax.script.*
import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.persistance.*

import org.json.simple.JSONObject

@Log
class MarcFieldLabelerIndexFormatConverter extends BasicOrderedPlugin implements IndexFormatConverter, WhelkAware {

    int order = 100
    Whelk whelk

    @Override
    List<Document> convert(Document doc) {
        return convert([doc])
    }

    @Override 
    List<Document> convert(List<Document> docs) {
        def outdocs = []    
        for (doc in docs) {
        }
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
