/**
 * Batch job for "digibest"
 */

reproductionAgent = [(TYPE): 'Agent', label: 'Kungliga biblioteket']

PARAMS = [
  bild: [
    //subUrn: 'dig',
    imageNote: 'Bild',
    reproductionAgent: reproductionAgent
  ],

  eod: [
    //subUrn: 'eod',
    bibliographyCode: 'EOD',
    reproductionAgent: reproductionAgent
  ],

  handskrift: [
    //subUrn: 'handskrift',
    bibliographyCode: 'DIHS',
    imageNote: 'Frampärm/första sida',
    reproductionAgent: reproductionAgent
  ],

  karta: [
    //subUrn: 'dig',
    imageNote: 'Karta/kartbok',
    reproductionAgent: reproductionAgent
  ],

  riks: [
    //subUrn: 'riks',
    bibliographyCode: 'RIKS',
    contentAccessibility: [ ["@id": "https://id.kb.se/a11y/textual"] ],
    reproductionAgent: 'Riksdagsbiblioteket och Kungliga biblioteket'
  ],

  vardagstryck: [
    //subUrn: 'vardagstryck',
    imageNote: 'Titelsida',
    hasNote: ["@type": "marc:SourceOfDescriptionNote", label: "S: Digitaliserat vardagstryck"],
    reproductionAgent: reproductionAgent
  ],

  tryck: [
    //subUrn: 'dig',
    reproductionAgent: reproductionAgent
  ],

  publ: [
    //subUrn: 'publ',
    imageNote: 'Titelsida',
    // NOTE: no production.typeNote (implies no associatedMediaUri?)
    reproductionAgent: null
    // TODO: optional thumbnailUrl ...
  ]
]

def makeParams(jobType) {

    return params
}

def jobs = [:]

[
    ['VardagstryckLaddfil.csv', 'vardagstryck'],
    ['TryckLaddfil.csv', 'tryck'],
    ['KartorLaddfil.csv', 'karta'],
    ['HSLaddfil.csv', 'handskrift'],
    //['AffischLaddfil.csv', 'affisch'],
    ['BildLaddfil.csv', 'bild'],
].each { csvFilePath, jobType ->
    new File(scriptDir, csvFilePath).readLines().drop(1).each {
        def (
            printid,
            bibliography,
            reproductionDate,
            associatedMediaUri,
            imageUrl,
            imageNote,
            cataloguersNote
        ) = (it + ';').split(';', -1)

        printid = printid ==~ /\d{5,11}/ ?
                  "http://libris.kb.se/resource/bib/${printid}" :
                  printid

        def params = [:] + PARAMS[jobType]

        params.bibliographyCode = bibliography
        params.year = reproductionDate
        params.mediaObjectUri = associatedMediaUri
        params.imageUrl = imageUrl
        params.imageNote = imageNote
        params.heldById = 'https://libris.kb.se/library/S'
        params.holdingNote = cataloguersNote

        println(printid)
        jobs[printid] = params
    }
}

selectByIds(jobs.keySet() as List) { docItem ->
    def params = jobs[docItem.doc.id] ?: jobs[docItem.doc.shortId]
    if (params.is(null)) {
        params = docItem.graph[1].sameAs.findResult { jobs[it[ID]] }
    }
    println(docItem.doc.id)
    script('process.groovy')(this, docItem, params)
}
