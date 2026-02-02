// MUST be run with --allow-loud

File bibids = new File(scriptDir, "KB_852_856.txt")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
    String fuzzyID = operation.trim().toUpperCase()

    if (fuzzyID.matches("[0-9X]{9,14}")) {
        where = """id in
        (select lb.id
        from lddb lb
        where lb.collection = 'bib' and
        lb.data#>'{@graph,0,identifiedBy}' @> '[{\"@type\": \"LibrisIIINumber\", \"value\":\"$fuzzyID\"}]'
    )"""
    }
    else {
        where = """id in
        (select lb.id
        from lddb lb
        where lb.collection = 'bib' and
        lb.data#>>'{@graph,0,controlNumber}' = '${fuzzyID}'
    )"""

    }

selectBySqlWhere(where) { bib ->
    bib.scheduleSave(loud: true)
}

}
