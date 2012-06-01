package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*

abstract class BasicFormatConverter implements FormatConverter {
    boolean enabled = true
    void enable() { this.enabled = true }
    void disable() { this.enabled = false }
    Whelk whelk
}

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
    public Document convert(Document doc, String mimeType, String format, String profile) {
        if (python == null) {
            throw new WhelkRuntimeException("Unable to find script engine for python.")
        }
        try {
            System.out.println("Converter executing script "+ this.scriptName)
            Reader r = null
            if (this.scriptName.startsWith("/")) {
                r = new FileReader(scriptName)
            } else {
                InputStream is = this.getClass().getClassLoader().getResourceAsStream(this.scriptName)
                r = new InputStreamReader(is)
            }
            if (python != null) {
                System.out.println("Plugin has whelk: " + this.whelk.getName())
                python.put("whelk", this.whelk)
                python.put("document", doc)
                python.eval(r)
                Object result = python.get("result")
                System.out.printf("\tScript result: %s\n", result)
                if (result != null) {
                    return doc.withData(((String)result).getBytes())
                } else {
                    return null
                }
            } else {
                System.out.println("Sorry, python is null")
            }
        } catch (ScriptException se) {
            se.printStackTrace()
        } catch (Exception e) {
            throw new WhelkRuntimeException(e)
        }
        return doc
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        System.out.println("Calling pythonrunner init method")
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
