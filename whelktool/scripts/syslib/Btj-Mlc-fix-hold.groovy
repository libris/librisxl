File input = new File(scriptDir, "burk_to_libris_log_finns_ej.txt")
List<String> ProgramLines = input.readLines()

for (String operation : ProgramLines) {
    String burkID = operation.trim()

where = """id in
            (SELECT lh.id
            FROM lddb lh
            WHERE lh.collection = 'hold'
            AND lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Mlc'
            AND CAST(lh.created AS date) = '2022-08-24%'
            AND lh.data#>>'{@graph,1,identifiedBy,0,value}' LIKE '${burkID}%'
        )"""
                
    selectBySqlWhere(where, { hold ->
        hold.scheduleDelete()
    })
}