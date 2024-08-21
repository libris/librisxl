/*
 * This removes marc:typeOfControl from all bib adminMetadata (4.9k+). Most are invalid values.
 *
 * See FMT-229 for more info.
 *
 */

selectBySqlWhere("""
        collection = 'bib' AND
        data#>>'{@graph,0,marc:typeOfControl}' IS NOT NULL
    """) { data ->

    def (record, thing) = data.graph

    record.remove('marc:typeOfControl')
    
    data.scheduleSave()
}
