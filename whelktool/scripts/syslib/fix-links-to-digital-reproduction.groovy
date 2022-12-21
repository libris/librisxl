File bibids = new File(scriptDir, "eod_som_ska_rattas_i_libris_2022_11.txt")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
    String[] part = operation.split('\t')
    String ctrlnr = part[0].trim()
    String oldCtrlnr = part[1].trim()
    String newCtrlnr = part[2].trim()

    def where = """id in
    (select lb.id
    from lddb lb
    where lb.collection = 'bib' and
    lb.data#>>'{@graph,0,controlNumber}' = '${ctrlnr}'
)"""

    selectBySqlWhere(where, silent: false, { bib ->
        def oPF = bib.graph[1]["otherPhysicalFormat"][0]["describedBy"][0]["controlNumber"]
        def physicalId = bib.doc.getShortId()
        boolean modified = false

        if (oPF == oldCtrlnr) {
            bib.graph[1].remove('otherPhysicalFormat')
            modified = true
        }

        if (modified) {
            bib.scheduleSave(loud: true)
        }

        selectByIds(['${newCtrlnr}'], { data ->
            def digiOPF = data.graph[1]["otherPhysicalFormat"][0]["describedBy"][0]["controlNumber"]
            boolean digimodified = false

            if (digiOPF == newCtrlnr) {
                data.graph[1].remove('otherPhysicalFormat')
                data.graph[1]["reproductionOf"] = []
                data.graph[1]["reproductionOf"] <<
                        ["@id": baseUri.toString() + "$physicalId#it"]
                digimodified = true
            }

            if (digimodified) {
                data.scheduleSave(loud: true)
            }
        })
    })
}