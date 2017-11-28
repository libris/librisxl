import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.component.PostgreSQLComponent


def sourceDump = args[0]
def databaseUrl = args[1]

def mapper = new ObjectMapper()

def storage = new PostgreSQLComponent(databaseUrl, "lddb", null, null)

def i = 1
new File(sourceDump).eachLine('UTF-8') {
    def data = mapper.readValue(it, Map)
    def id = data.descriptions.entry['@id']
    def doc = new Document(id, data, [dataset: 'auth'])
    storage.createDocument(doc)
    println "Stored #${i++} as ${id}"
}
