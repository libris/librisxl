PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = """
        collection = 'auth' AND (
        data#>>'{@graph,1,@type}' = 'Topic' OR
        data#>>'{@graph,1,@type}' = 'ComplexSubject' OR
        data#>>'{@graph,1,@type}' = 'Temporal' OR
        data#>>'{@graph,1,@type}' = 'Geographic' OR
        data#>>'{@graph,1,@type}' = 'TopicSubdivision')
        AND (
        data#>>'{@graph,1,inScheme,@id}' = 'https://id.kb.se/term/barn' OR
        data#>>'{@graph,1,inScheme,@id}' = 'https://id.kb.se/term/sao')
        AND NOT (
        data#>>'{@graph,0,mainEntity}' LIKE '%id.kb.se%')
        """

selectBySqlWhere(where) { data ->
    String librisUri = data.graph[1]['@id']
    String idkbseUri = data.graph[1].inScheme['@id'] + '/' + data.graph[1].prefLabel

    data.graph[1]['@id'] = idkbseUri
    data.graph[1].sameAs += ['@id':librisUri]
    data.graph[0].mainEntity['@id'] = idkbseUri

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
