/*
 * This sets encodingLevel to marc:AbbreviatedLevel for all remaining bibliographic
 * records with no property encodingLevel.
 *
 * See LXL-2225 for more info.
 *
 */

String ENCODING_LEVEL_VALUE = 'marc:AbbreviatedLevel'
PrintWriter scheduledToSetEncodingLevel = getReportWriter("scheduled-to-set-encodingLevel")

selectBySqlWhere("""
        collection = 'bib' AND data#>>'{@graph,0,encodingLevel}' ISNULL
    """) { data ->
    // guard against missing entity
    if (data.graph.size() < 2)
        return

    def (record, instance, work) = data.graph

    record.put('encodingLevel', ENCODING_LEVEL_VALUE)
    scheduledToSetEncodingLevel.println("${data.graph[0][ID]}")
    data.scheduleSave()

}
