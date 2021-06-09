String where = """
    collection = 'bib'
    AND data#>>'{@graph,1,@type}' = 'Instance'
    AND data#>>'{@graph,1,instanceOf,@type}' = 'Text'
    AND data#>>'{@graph,1,issuanceType}' = 'Monograph'
    AND data#>'{@graph,1,identifiedBy}' @> '[{\"@type\":\"ISBN\"}]'
    AND NOT data#>'{@graph,1,instanceOf,genreForm}' @> '[{\"@id\":\"https://id.kb.se/term/saogf/Handskrifter\"}]'
"""

selectBySqlWhere(where) { data ->
    Map instance = data.graph[1]

    List isbn = instance.identifiedBy.findAll { it."@type" == "ISBN" }
    List ct = instance.carrierType
    List mt = instance.mediaType

    boolean modified

    if (qualifiersAreCompatible(isbn)) {
        // If carrierType or mediaType exists they must have compatible values
        if (isbn.size() == 1 && (!ct || carrierIsCompatible(ct)) && (!mt || mediaIsCompatible(mt))) {
            instance."@type" = "Print"
            modified = true
        }
        // At least one of carrierType and mediaType must exist and their values must be compatible
        else if (isbn.size() > 1 && (ct || mt) && !(ct && !carrierIsCompatible(ct) || mt && !mediaIsCompatible(mt))) {
            instance."@type" = "Print"
            modified = true
        }
    }

    if (modified)
        data.scheduleSave()
}

boolean carrierIsCompatible(List ct) {
    Set validTypes =
            [
                    "https://id.kb.se/marc/RegularPrintReproduction",
                    "https://id.kb.se/term/rda/Volume",
                    "https://id.kb.se/marc/LargePrint",
                    "https://id.kb.se/marc/RegularPrint",
                    "https://id.kb.se/marc/RegularPrintReproductionEyeReadablePrint",
                    "https://id.kb.se/marc/TextMaterialType-b"
            ] as Set

    return ct.every { it["@id"] in validTypes }
}

boolean mediaIsCompatible(List mt) {
    Set validTypes =
            [
                    "https://id.kb.se/term/rda/Unmediated",
                    "https://id.kb.se/marc/LargePrint",
                    "https://id.kb.se/marc/RegularPrint",
                    "https://id.kb.se/term/rda/Volume"
            ] as Set

    return mt.every { it["@id"] in validTypes }
}

boolean qualifiersAreCompatible(List isbn) {
    String validQualifierParts = ~/(?i)hard(back|cover)|h[bc]|paperback|pb|hä?f|inn?b|spiral|pocket|brosch|print/
    // "h." and "ib." are too generic to be considered valid if only part of the qualifier string
    String validEntireQualifiers = ~/(?i)h\.|ib\./

    return isbn.every {
        String q = it.qualifier in List ? it.qualifier[0] : it.qualifier
        return q && (q =~ validQualifierParts || q ==~ validEntireQualifiers)
    }
}



