/*

Alltid:
    Länk: https://digitalt.kb.se/<paket_id>
    Länktext: Tillgänglig via KB Digitalt (Kungliga biblioteket)

Om tidningar.kb.se finns med i services
    Länk: https://tidningar.kb.se/<libris_id>
    Länktext: Tillgänglig via Svenska tidningar (Kungliga biblioteket)

example:

p71505c13wh5fvj,dark-27606469,{data_platform}
bvnt3gzn329pmxt,dark-27606689,{data_platform}
2k4jc5tl0gtsvc0c,dark-24435514,"{data_platform,bildpiloten}"
wdzghsfpttlll0b4,dark-24447889,"{data_platform,bildpiloten}"
g2f3g2jqdgq90td8,dark-32571584,"{data_platform,tidningar.kb.se}"
vgtk451ds2mxt3h1,dark-32576047,"{data_platform,tidningar.kb.se}"
4q3rzthw2wzhnr3b,dark-32574142,"{data_platform,tidningar.kb.se}"
g2f5vrwpdvk97mw9,dark-32576116,"{data_platform,tidningar.kb.se}"

*/

var fileName = System.getProperty("links-csv")

var todo = [:]
for (var line : new File(fileName).readLines()) {
    var c = line.split(",", 3)
    var id = c[0]
    var dark = c[1]
    var hasTidningar = c[2].contains("tidningar.kb.se")
    todo[id] = [dark, hasTidningar]
}

selectByIds(todo.keySet()) { bib ->
    String id = bib.doc.shortId
    var (dark, hasTidningar) = todo[id]
    var associatedMedia = bib.graph[1].associatedMedia ?: []
    bib.graph[1].associatedMedia = associatedMedia

    var digi = [
            "@type" : "MediaObject",
            uri: [ "https://digitalt.kb.se/${dark}".toString() ],
            'marc:publicNote': 'Tillgänglig via KB Digitalt (Kungliga biblioteket)'
    ]
    if (!associatedMedia.contains(digi)) {
        associatedMedia.add(0, digi)
        bib.scheduleSave()
    }

    if (hasTidningar) {
        var tidningar = [
                "@type" : "MediaObject",
                uri: [ "https://tidningar.kb.se/${id}".toString() ],
                'marc:publicNote': 'Tillgänglig via Svenska tidningar (Kungliga biblioteket)'
        ]
        if (!associatedMedia.contains(tidningar)) {
            associatedMedia.add(0, tidningar)
            bib.scheduleSave()
        }
    }
}
