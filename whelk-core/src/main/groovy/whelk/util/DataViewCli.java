package whelk.util

import whelk.Document
import whelk.Whelk

import static whelk.util.Jackson.mapper

// TODO: factor out and use MarcFrameCli.addJsonLd to allow for local data.
assert 'xl.secret.properties' in System.properties

whelk = Whelk.createLoadedCoreWhelk()
ld = whelk.jsonld

def COMMANDS = ['card', 'chip', 'embellish', 'index']

def view(cmd, ref) {
    var file = new File(ref)
    def data = file.exists() ? mapper.readValue(file, Map) : null
    switch (cmd) {
        // TODO: most useful if local data can be loaded (see TODO above).
        //case 'card':
        //    break
        //case 'chip':
        //    break
        case 'embellish':
            if (data) {
              return ld.embellish(data)
            } else {
              var doc = whelk.getDocument(ref)
              assert doc, "$ref is not found?"
              System.err.println "$cmd: $doc.id (data is ${sizeOf(doc.data)} bytes)"
              whelk.embellish(doc)
              System.err.println "Done (data is now ${sizeOf(doc.data)} bytes)"
              return doc.data
            }
        case 'index':
            def doc = new Document(data)
            return whelk.elastic.getShapeForIndex(doc, whelk, null)
    }
}

def sizeOf(data) {
    return mapper.writeValueAsString(data).size()
}

List refs
def cmd = "card"

if (args.length > 1) {
    cmd = args[0]
    assert cmd in COMMANDS
    refs = args[1..-1]
} else {
    refs = args[0..-1]
}

for (ref in refs) {
    def result = view(cmd, ref)
    println mapper.writeValueAsString(result)
}
