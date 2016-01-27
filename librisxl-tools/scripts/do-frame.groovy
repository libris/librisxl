def (src, rootUri) = args as List

src = src ?: "/tmp/data.jsonld"
rootUri = rootUri ?: "https://libris.kb.se/tmp/marc.json"

def mapper = new org.codehaus.jackson.map.ObjectMapper()
def json = mapper.readValue(new File(src), Map)
def framed = whelk.JsonLd.frame(rootUri, json)

println mapper.defaultPrettyPrintingWriter().writeValueAsString(framed)
