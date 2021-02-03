package whelk

class ExternalReferences {

    /**
     * Returns true, if the supplied iri matches some rule or combination of rules
     * saying it is an external resource that should be trusted and cached within XL.
     */
    public static boolean iriIsWhitelisted(String iri) {
        if (iri.startsWith("http://id.loc.gov/authorities/"))
            return true
        return false
    }

    /**
     * Build an XL "document" (of some sort) out of whatever useful data could be
     * obtained at 'iri'.
     */
    public static Document buildXlCacheObject(String iri) {
        System.err.println("Attempting to construct representation of: " + iri)
        return new Document(["@graph":[
                [
                        "@id" : Document.BASE_URI.resolve(IdGenerator.generate()),
                        "mainEntity" : ["@id" : iri],
                ],
                [
                        "@id": iri,
                        "prefLabel" : "I'm a cached external resource!"
                ]
        ]])
    }
}
