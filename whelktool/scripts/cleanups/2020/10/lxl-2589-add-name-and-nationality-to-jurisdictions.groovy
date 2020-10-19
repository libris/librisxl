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

    Script.modified.println("${auth.graph[0][ID]}")
    auth.scheduleSave()
}