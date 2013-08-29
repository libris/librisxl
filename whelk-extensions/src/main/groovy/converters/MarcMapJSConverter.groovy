package se.kb.libris.whelks.plugin

import org.mozilla.javascript.Context
import org.mozilla.javascript.Scriptable
import org.mozilla.javascript.NativeJSON

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*

@Log
class MarcMapJSConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String id = this.class.name
    boolean enabled = true
    void enable() { this.enabled = true }
    void disable() { this.enabled = false }

    Object marcmap
    String objName
    String funcName
    Context cx = Context.enter()
    Scriptable scope = cx.initStandardObjects()

    int order = 0
    String requiredContentType = "application/x-marc-json"

    MarcMapJSConverter(scriptPath, objName, funcName) {
        this(null, scriptPath, objName, funcName)
    }

    MarcMapJSConverter(marcmapPath, scriptPath, objName, funcName) {
        def repr = null
        if (marcmapPath) {
            repr = new File(marcmapPath).text
        } else {
            repr = this.class.classLoader.getResourceAsStream(
                    "marcmap.json").getText('utf-8')
        }
        marcmap = parseJSON(repr)
        this.objName = objName
        this.funcName = funcName
        String scriptText = new File(scriptPath).getText('utf-8')
        cx.evaluateString(scope, scriptText, scriptPath, 1, null)
    }

    void shutdown() {
        cx.exit()
    }

    @Override
    @Override
    List<IndexDocument> doConvert(Document doc) {
        def outdocs = []
        def struct = parseJSON(doc.dataAsString)
        def obj = scope.get(objName, scope)
        def func = obj.get(funcName, obj)
        def map = marcmap.get('bib', marcmap)
        def result = func.call(cx, scope, obj, [map, struct] as Object[])
        def repr = NativeJSON.stringify(cx, scope, result, null, 2)
        outdocs << new IndexDocument().withData(repr).withIdentifier(doc.identifier)
        return outdocs
    }

    def parseJSON(String repr) {
        def obj = NativeJSON.parse(cx, scope, repr)
        return obj
    }

}
