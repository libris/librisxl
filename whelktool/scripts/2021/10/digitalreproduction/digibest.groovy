/**
 * Batch job for "digibest"
 */

reproductionComment = 'Digitalt faksimil'
reproductionAgentLabel = 'Kungliga biblioteket'

PARAMS = [
  bild: [
    subUrn: 'dig',
    tumnagelNote: 'Bild',
    reproductionComment: reproductionComment,
    reproductionAgentLabel: reproductionAgentLabel
  ],

  eod: [
    subUrn: 'eod',
    bibliographyCode: 'EOD',
    reproductionComment: reproductionComment,
    reproductionAgentLabel: reproductionAgentLabel
  ],

  handskrift: [
    subUrn: 'handskrift',
    bibliographyCode: 'DIHS',
    tumnagelNote: 'Frampärm/första sida',
    reproductionComment: reproductionComment,
    reproductionAgentLabel: reproductionAgentLabel
  ],

  karta: [
    subUrn: 'dig',
    tumnagelNote: 'Karta/kartbok',
    reproductionComment: reproductionComment,
    reproductionAgentLabel: reproductionAgentLabel
  ],

  riks: [
    subUrn: 'riks',
    bibliographyCode: 'RIKS',
    reproductionComment: 'Digitalt faksimil och elektronisk text',
    reproductionAgentLabel: 'Riksdagsbiblioteket och Kungliga biblioteket'
  ],

  vardagstryck: [
    subUrn: 'vardagstryck',
    tumnagelNote: 'Titelsida',
    hasNote: ["@type": "marc:SourceOfDescriptionNote", label: "S: Digitaliserat vardagstryck"],
    reproductionComment: reproductionComment,
    reproductionAgentLabel: reproductionAgentLabel
  ],

  tryck: [
    subUrn: 'dig',
    reproductionComment: reproductionComment,
    reproductionAgentLabel: reproductionAgentLabel
  ],

  publ: [
    subUrn: 'publ',
    tumnagelNote: 'Titelsida',
    reproductionComment: null,
    reproductionAgentLabel: null
    // TODO: optional thumbnailUrl ...
  ]
]

def makeParams(jobType) {
    def params = [:] + PARAMS[jobType]

    params.year = (1900 + new Date().getYear()).toString()

    def urnCode = 'N/A' // TODO: materialtyp != "8" ? librisid_old : prompt("Ange nummer för publikationen (publ-?):"
    params.mediaObjectUri = "https://urn.kb.se/resolve?urn=urn:nbn:se:kb:${params.subUrn}-${urnCode}"

    /* TODO:
    sysNr="curl -s "https://ask.kb.se/F/?func=find-c&ccl_term=libnr%3D+${librisid_old}" | grep Gr1Sysnr | sed "s/  <dt id='Gr1Sysnr'>//" | sed "s/<\/dt>//" | sed "s/ //""
    tumnagel=$(curl -s "https://weburn.kb.se/tumnaglar/${sysNrDir}/" | grep "${sysNr}" | sed -e 's/^<tr.*href="//' | sed -e 's/".* //')
    params.thumbnailUrl = "https://weburn.kb.se/tumnaglar/${sysNrDir}/${tumnagel}"
    */
    params.thumbnailUrl = null

    params.heldById = 'https://libris.kb.se/library/S'
    // TODO: |= <kbeodbest>, <kbdtryck>, <kbdkart>, <kbdbild>
    params.holdingNote = "kb${params.subUrn}best"

    return params
}

// TODO: hardcoded local test of one item
def jobs = [
  'http://libris.kblocalhost.kb.se/n6038s6054qvfr6': makeParams('eod')
]

selectByIds(jobs.keySet() as List) { doc ->
    def params = jobs[doc.id]
    script('process.groovy')(doc, params)
}
