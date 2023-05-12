package datatool.scripts.mergeworks

import whelk.Document
import whelk.IdGenerator
import whelk.Whelk
import whelk.exception.WhelkRuntimeException
import whelk.util.LegacyIntegrationTools

class NewWork extends MergedWork {

    NewWork(Collection<Doc> derivedFrom, File reportDir) {
        this.derivedFrom = derivedFrom
        this.reportDir = new File(reportDir, 'new')
        this.workPath = ['@graph', 1]
    }

    @Override
    void store(Whelk whelk) {
        if (!whelk.createDocument(document, changedIn, changedBy,
                LegacyIntegrationTools.determineLegacyCollection(document, whelk.getJsonld()), false)) {
            throw new WhelkRuntimeException("Could not store new work: ${document.shortId}")
        }
    }

    void createDoc(Whelk whelk, Map workData) {
        this.document = buildWorkDocument(workData)
        this.doc = new Doc(whelk, document)
    }

    private Document buildWorkDocument(Map workData) {
        String workId = IdGenerator.generate()
        def reportUri = "http://xlbuild.libris.kb.se/works/${reportDir.getPath()}/${workId}.html"

        workData['@id'] = "TEMPID#it"
        Document d = new Document([
                "@graph": [
                        [
                                "@id"          : "TEMPID",
                                "@type"        : "Record",
                                "mainEntity"   : ["@id": "TEMPID#it"],
                                "technicalNote": [[
                                                          "@type"  : "TechnicalNote",
                                                          "hasNote": [[
                                                                              "@type": "Note",
                                                                              "label": ["Maskinellt utbrutet verk... TODO"]
                                                                      ]],
                                                          "uri"    : [reportUri]
                                                  ]
                                ]],
                        workData
                ]
        ])

        d.deepReplaceId(Document.BASE_URI.toString() + workId)
        return d
    }


}
