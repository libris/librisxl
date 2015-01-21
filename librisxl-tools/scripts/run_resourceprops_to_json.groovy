/* Script to convert properties to json
 Example usage:
 $ gradle -q groovy -Dargs="scripts/run_resourceprops_to_json.groovy <filename.properties> <filename.json>"
*/
import org.codehaus.jackson.map.ObjectMapper

def mapper = new ObjectMapper()
def propFile = args[0]
def jsonFile = args[1]

println "Converting properties $propFile to json..."
def outjson = [:]
def properties = new Properties()
properties.load(this.getClass().getClassLoader().getResourceAsStream(propFile))
properties.each { key, value ->
    outjson[key] = value
}
def file = new File("src/main/resources/" + jsonFile)
file << mapper.defaultPrettyPrintingWriter().writeValueAsString(outjson).getBytes("utf-8")
println "Created src/main/resources/$jsonFile"
