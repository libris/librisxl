package se.kb.libris.whelks.plugin;

import javax.script.*;
import java.io.*;

import java.util.List;

import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.exception.*;

public class PythonRunnerFormatConverter implements FormatConverter {

    private boolean enabled = true;
    private Whelk whelk = null;
    private String scriptName = null;

    final private ScriptEngine python = new ScriptEngineManager().getEngineByName("python");

    PythonRunnerFormatConverter(String scriptName) {
        this.scriptName = scriptName;
        if (python == null) {
            throw new WhelkRuntimeException("Unable to find script engine for python.");
        }
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

    @Override
    public Document convert(Document doc, String mimeType, String format, String profile) {
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

    public void enable() { this.enabled = true; }
    public void disable() { this.enabled = false; }

    public void setWhelk(Whelk w) { this.whelk = w; }
}
