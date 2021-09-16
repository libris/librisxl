package whelk.rest.api

import whelk.JsonLd

class SiteData {
    static final String IDKBSE = 'https://id.kb.se/'
    static final String LIBRIS = 'https://libris.kb.se/'
    static final Map SITES = [
            (IDKBSE): [
                    (JsonLd.ID_KEY)  : IDKBSE,
                    (JsonLd.TYPE_KEY): "DataCatalog",
                    "title"          : "id.kb.se",
                    "summary"        : [ID: "/doc/summary"],
                    "stylesheet"     : ["name": "id.css"],
                    "statsindex"     : '{"inScheme.@id":{"inCollection.@id":["@type"], "@type":[]}}',
                    "statsfind"      : '{"inScheme.@id":{"inCollection.@id":["@type"], "@type":[]}}',
                    "boost"          : 'id.kb.se',
                    "filter_param"   : "inScheme.@id",
                    "applyInverseOf" : true,
                    "itemList"       : [
                            [ID: "/", "title": "Sök"],
                            [ID: "/marcframe/", "title": "MARC-mappningar"],
                            [ID: "/vocab/", "title": "Basvokabulär"],
                            [ID: "/doc/about", "title": "Om id.kb.se"],
                    ]
            ],
            (LIBRIS): [
                    (JsonLd.ID_KEY): LIBRIS,
                    (JsonLd.TYPE_KEY): "DataCatalog",
                    "title": "libris.kb.se",
                    "summary": ["articleBody": "<p>Data på <b>LIBRIS.KB.SE</b>.</p>"],
                    "statsindex": '{"@type": []}',
                    "filter_param": "@type",
                    //"stats": ["@type":["meta.bibliography.@id":["publication.providerDate":[]]]]
                    "statsfind":
                            """
            {
                "@reverse.itemOf.heldBy.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":1000
                },
                "instanceOf.language.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "carrierType.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "instanceOf.@type":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "publication.year":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":500
                },
                "issuanceType":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "meta.encodingLevel":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "@type":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "inScheme.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "inCollection.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "genreForm.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "instanceOf.genreForm.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },

                "contentType.@id": {
                    "sort":"value",
                    "sortOrder": "desc",
                    "size":100
                },
                "nationality.@id": {
                    "sort":"value",
                    "sortOrder": "desc",
                    "size":100
                },
                "language.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "contribution.agent.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":20
                },
                "instanceOf.subject.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                },
                "meta.bibliography.@id":{
                    "sort":"value",
                    "sortOrder":"desc",
                    "size":100
                }
            }
        """,
                    "itemList": [
                    ]
            ]
    ]
}
