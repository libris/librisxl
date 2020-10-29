PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "(collection = 'bib' or collection = 'hold') and ( " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/MimerProd' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/Mimer' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/KBDIGI' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/oden' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/MimerProdReadonly' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/GUProd' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/GU' OR " +
        "data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/umu'" +
        ")"

Map creatorMap = [
        "https://libris.kb.se/library/MimerProd" : "https://libris.kb.se/library/S",
        "https://libris.kb.se/library/Mimer" : "https://libris.kb.se/library/S",
        "https://libris.kb.se/library/KBDIGI" : "https://libris.kb.se/library/S",
        "https://libris.kb.se/library/oden" : "https://libris.kb.se/library/S",
        "https://libris.kb.se/library/MimerProdReadonly" : "https://libris.kb.se/library/S",
        "https://libris.kb.se/library/GUProd" : "https://libris.kb.se/library/G",
        "https://libris.kb.se/library/GU" : "https://libris.kb.se/library/G",
        "https://libris.kb.se/library/umu" : "https://libris.kb.se/library/Um",
]

selectBySqlWhere(where) { data ->

    String oldCreator = data.graph[0]["descriptionCreator"]["@id"]
    String newCreator = creatorMap[oldCreator]
    if (newCreator == null) {
        failedUpdating.println("Failed to update ${data.doc.shortId} due to descriptionCreator missmatch: " +
                data.graph[0]["descriptionCreator"]["@id"])
        return
    }

    data.graph[0]["descriptionCreator"]["@id"] = newCreator

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
