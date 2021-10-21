package whelk.util

import whelk.Document
import whelk.Whelk

import static whelk.util.Jackson.mapper

// TODO: factor out and use MarcFrameCli.addJsonLd to allow for local data.
assert 'xl.secret.properties' in System.properties

whelk = Whelk.createLoadedSearchWhelk()
ld = whelk.jsonld

def COMMANDS = ['card', 'chip', 'embellish', 'index']

def view(cmd, source) {
    def result
    switch (cmd) {
        // TODO: most useful if local data can be loaded (see TODO above).
        //case 'card':
        //    break
        //case 'chip':
        //    break
        case 'embellish':
            return mapper.writeValueAsString(ld.embellish(source))
        case 'index':
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
    def source = mapper.readValue(new File(fpath), Map)
    def result = view(cmd, source)
    println result
}
