package se.kb.libris.whelks.plugin;

import javax.script.*;
import java.io.*;

import java.util.List;
import org.json.simple.JSONObject;

import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.exception.*;
import se.kb.libris.whelks.persistance.*;

public class PythonRunnerFormatConverter implements FormatConverter, JSONSerialisable, JSONInitialisable {

    private boolean enabled = true;
    private Whelk whelk = null;
    private String scriptName = null;

    final private ScriptEngine python = new ScriptEngineManager().getEngineByName("python");

    public PythonRunnerFormatConverter() {}

    public PythonRunnerFormatConverter(String scriptName) {
        this.scriptName = scriptName; 
    }

    private void listAvailableEngines() {
        try {
            ScriptEngineManager mgr = new ScriptEngineManager();
            List<ScriptEngineFactory> factories = mgr.getEngineFactories();
            for (ScriptEngineFactory factory: factories) {
                System.out.println("ScriptEngineFactory Info");
                String engName = factory.getEngineName();
                String engVersion = factory.getEngineVersion();
                String langName = factory.getLanguageName();
                String langVersion = factory.getLanguageVersion();
                System.out.printf("\tScript Engine: %s (%s)\n",
                        engName, engVersion);
                List<String> engNames = factory.getNames();
                for(String name: engNames) {
                    System.out.printf("\tEngine Alias: %s\n", name);
                }
                System.out.printf("\tLanguage: %s (%s)\n",
                        langName, langVersion);
            }
        } catch (Exception e) {
            throw new WhelkRuntimeException(e);
        }
    }

    public void setScriptName(String sn) {this.scriptName = sn;}
    public String getScriptName() {return this.scriptName;}

    @Override
    public Document convert(Document doc, String mimeType, String format, String profile) {
        if (python == null) {
            throw new WhelkRuntimeException("Unable to find script engine for python.");
        }
        try {
            Reader r = null;
            if (this.scriptName.startsWith("/")) {
                System.out.println("Reading from filesystem");
                r = new FileReader(scriptName);
            } else {
                r = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(this.scriptName));
            }
            if (python != null) {
                python.put("whelk", this.whelk);
                python.put("document", doc);
                python.eval(r);
                Object result = python.get("result");
                System.out.printf("\tScript result: %s\n", result);
                if (result != null) {
                    return doc.withData(((String)result).getBytes());
                } else {
                    return null;
                }
            } else {
                System.out.println("Sorry, python is null");
            }
        } catch (ScriptException se) {
            se.printStackTrace();
        } catch (Exception e) {
            throw new WhelkRuntimeException(e);
        }
        return doc;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getId() {
        return "pythonRunnerFormatConverter";
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        System.out.println("Calling pythonrunner init method");
        try {
            this.scriptName = obj.get("scriptName").toString();
        } catch (Exception e) {
            throw new WhelkRuntimeException(e);

        }
        return this;

    }

    @Override
    public JSONObject serialize() {
        JSONObject _converter = new JSONObject();
        _converter.put("_classname", this.getClass().getName());
        _converter.put("scriptName", this.scriptName);
                
        return _converter;
    }

    public void enable() { this.enabled = true; }
    public void disable() { this.enabled = false; }

    public void setWhelk(Whelk w) { this.whelk = w; }

}
