package datatool.scripts.mergeworks

import whelk.Document
import whelk.IdGenerator

class NewWork implements MergedWork {
    Document doc
    Collection<Doc> derivedFrom
    File reportDir

    NewWork(Map data, Collection<Doc> derivedFrom, File reportDir) {
        this.derivedFrom = derivedFrom
        this.reportDir = new File(reportDir, 'new')
        this.doc = buildWorkDocument(data)
    }

    private Document buildWorkDocument(Map workData) {
        String workId = IdGenerator.generate()
        def reportUri = "http://xlbuild.libris.kb.se/works/${reportDir.getPath().replace('report/', '')}/${workId}.html"

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
