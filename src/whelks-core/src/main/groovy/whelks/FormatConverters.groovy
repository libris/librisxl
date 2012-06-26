package se.kb.libris.whelks.plugin

import javax.script.*
import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.persistance.*

import org.json.simple.JSONObject

abstract class BasicFormatConverter implements FormatConverter {
    boolean enabled = true
    void enable() { this.enabled = true }
    void disable() { this.enabled = false }
    Whelk whelk
}

@Log
class PythonRunnerFormatConverter extends BasicFormatConverter implements JSONSerialisable, JSONInitialisable {

    final private ScriptEngine python = new ScriptEngineManager().getEngineByName("python")
    String scriptName
    String id = "PythonRunnerFormatConverter"

    PythonRunnerFormatConverter() {}
    PythonRunnerFormatConverter(String sn) { this.scriptName = sn }

    private void listAvailableEngines() {
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

    @Override
    public Document convert(Document doc) { 
        if (python == null) {
            throw new WhelkRuntimeException("Unable to find script engine for python.")
        }
        try {
            log.debug("Converter executing script "+ this.scriptName)
            Reader r = null
            if (this.scriptName.startsWith("/")) {
                r = new FileReader(scriptName)
            } else {
                InputStream is = this.getClass().getClassLoader().getResourceAsStream(this.scriptName)
                r = new InputStreamReader(is)
            }
            if (r == null) {
                throw new WhelkRuntimeException("Failed to read script.")
            }
            python.put("whelk", whelk)
            python.put("document", doc)
            python.eval(r)
            Object result = python.get("result")
            if (result != null) {
                return doc.withData(((String)result).getBytes())
            } else {
                return null
            }
        } catch (ScriptException se) {
            log.error("Script failed: " + se.getMessage(), se)
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)
        }
        return doc
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


}
