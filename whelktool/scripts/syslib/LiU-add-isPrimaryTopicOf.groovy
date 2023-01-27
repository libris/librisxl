File input = new File(scriptDir, "liu-add-link-ids.csv")
List<String> ProgramLines = input.readLines().drop(1)

def itemList = []

for (String operation : ProgramLines) {

    String[] part = operation.split(';', -1)
    String title = part[0].trim()
    String ctrlnr = part[1].trim()
    String url = part[2].trim()

    def where = """id in 
    (select lb.id
    from lddb lb
    left join lddb lh on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'
    where lb.collection = 'bib' and
    lb.data#>>'{@graph,0,controlNumber}' = '${ctrlnr}'
    and lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Li'
)"""

    selectBySqlWhere(where, { bib ->
        def instance = bib.graph[1]
        if (!instance["isPrimaryTopicOf"]) {
            instance["isPrimaryTopicOf"] = []
            instance["isPrimaryTopicOf"] <<
                    [
                            "uri"            : ["${url}"],
                            "@type"          : "Document",
                            "marc:publicNote": "Sammanfattning från Linköping University Electronic Press"
                    ]
        }

        bib.scheduleSave(loud: true)
    })
}