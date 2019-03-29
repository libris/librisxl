package whelk.util

import whelk.Document
import whelk.JsonLd
import whelk.Whelk

// TODO: factor out and use MarcFrameCli.addJsonLd?
assert 'xl.secret.properties' in System.properties

whelk = Whelk.createLoadedSearchWhelk()
ld = whelk.jsonld

def COMMANDS = ['card', 'chip', 'embellish', 'index']

def view(cmd, source) {
    def result
    switch (cmd) {
        case 'card':
            break
        case 'chip':
            break
        case 'embellish':
            return ld.embellish(source)
        case 'index':
            def coll = null
            def doc = new Document(source)
            return whelk.elastic.getShapeForIndex(doc, whelk, null)
    }
}

List fpaths
def cmd = "card"

if (args.length > 1) {
    cmd = args[0]
    assert cmd in COMMANDS
    fpaths = args[1..-1]
} else {
    fpaths = args[0..-1]
}

for (fpath in fpaths) {
    def source = ld.mapper.readValue(new File(fpath), Map)
    def result = view(cmd, source)
    println ld.mapper.writeValueAsString(result)
}
