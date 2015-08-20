import org.codehaus.jackson.map.ObjectMapper
import whelk.JsonDocument
import whelk.component.PostgreSQLStorage


def sourceDump = args[0]
def databaseUrl = args[1]

def mapper = new ObjectMapper()

def storage = new PostgreSQLStorage(
        ["databaseUrl": databaseUrl, "tableName": "lddb"])

storage.onStart()
storage.componentBootstrap(null)

def i = 1
new File(sourceDump).eachLine('UTF-8') {
    def contents = mapper.readValue(it, Map).contents
    def id = contents.entry['@id']
    def doc = new JsonDocument()
        .withIdentifier(id)
        .withData(contents)
        .withModified(new Date().time)
        .withDataset('auth')
    storage.store(doc)
    println "Stored #${i++} as ${id}"
}
