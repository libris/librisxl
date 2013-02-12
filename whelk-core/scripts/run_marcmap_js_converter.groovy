/* Example usage:
    $ gradle -q groovy -Dargs="scripts/run_marcmap_js_converter.groovy \
            src/main/resources/marcmap.json \
            ../../../kitin/static/js/marcjson.js marcjson.rawToNamed \
            ../../../kitin/examples/bib/7149593.json"
*/
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*

def (marcmap, script, callExpr, data) = args
def slug = (data =~ /\/(\w+\/\w+)\.json$/)[0][1]
def id = "tag:data.kb.se,2012:/${slug}"
def (obj, func) = callExpr.split(/\./)

def i = args.length > 4? args[4] as int : 1

def conv = new MarcMapJSConverter(marcmap, script, obj, func)

def convert = {
    def source = new BasicDocument().withIdentifier(id).withData(new File(data).text)
    return conv.convert(source)
}

if (i == 1) {
    println groovy.json.JsonOutput.prettyPrint(convert().dataAsString)
} else {
    println "Start..."
    def start = System.currentTimeMillis()
    i.times convert
    println "Running ${i} times took ${(System.currentTimeMillis() - start) / 1000} s."

}
