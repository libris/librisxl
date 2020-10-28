class Script {
    static PrintWriter modified
    static PrintWriter errors
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")

query = "collection = 'auth' AND data#>>'{@graph,1,@type}' = 'Jurisdiction'"

selectBySqlWhere(query) { auth ->
    try {
        process(auth)
    }
    catch(Exception e) {
        Script.errors.println("${auth.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(auth) {
    if (!auth.graph[1]["marc:subordinateUnit"] || auth.graph[1]["isPartOf"]) {
        return
    }

    auth.graph[1]["isPartOf"] = [
            "@type": "Jurisdiction",
            "name" : "Sverige"
    ]

    auth.graph[1]["nationality"] = [
            [
                    "@id": "https://id.kb.se/nationality/e-sw---"
            ]
    ]

    def variant = auth.graph[1]["hasVariant"]

    variant = variant ?: []

    variant.each {
        if (it["@type"] == "Jurisdiction") {
            it["@type"] = "Organization"

            if (it["marc:subordinateUnit"]) {
                it["name"] = it["marc:subordinateUnit"].join(" ")
                it.remove("marc:subordinateUnit")
            }
        }
    }

    variant << [
            "@type": "Organization",
            "name" : auth.graph[1]["marc:subordinateUnit"][0]
    ]

    auth.graph[1]["hasVariant"] = variant

    Script.modified.println("${auth.graph[1]["hasVariant"]}")
    Script.modified.println("${auth.graph[0][ID]}")
    auth.scheduleSave()
}