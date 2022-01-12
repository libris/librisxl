/*
See LXL-3739 for more info.

URI for <label> obtained from location response header of "curl -I -o headers -s ${url}"
where url="https://id.loc.gov/authorities/subjectheadings/label/<label>"

Manually edited (spelling mistakes etc):

Darkwawe (music) -> Darkwave (Music) (in pm14bfp71ldshtn)
Taraab -> Taraab (Music) (in wt7bjpmf1s02t5n)
Occational verse -> Occasional verse (in khw087h30db2179)
Alternative histories (fiction) -> Alternative histories (Fiction) (in 0xbdhl4j3g28rlz)
Straight-edge -> Straight-edge (Music) (in 42gkrt7n4r3cqv8)
Indians Music -> Indians--Music (in 86lpwfvs37m8dj2)
Minutes (records) -> Minutes (Records) (in 0xbfnmfj240rck1)

*/

import whelk.util.DocumentUtil

LABEL_TO_URI = ["Action and adventure fiction"      : "https://id.loc.gov/authorities/genreForms/gf2014026217",
                "Administrative regulations"        : "https://id.loc.gov/authorities/genreForms/gf2011026030",
                "Aeolian harp"                      : "https://id.loc.gov/authorities/subjects/sh85001245",
                "Allegories"                        : "https://id.loc.gov/authorities/childrensSubjects/sj2021051655",
                "Alternative histories (fiction)"   : "https://id.loc.gov/authorities/genreForms/gf2014026220.html",
                "Arthurian romances"                : "https://id.loc.gov/authorities/childrensSubjects/sj96006040",
                "Audiobooks"                        : "https://id.loc.gov/authorities/genreForms/gf2011026063",
                "Autobiographical fiction"          : "https://id.loc.gov/authorities/genreForms/gf2014026231",
                "Ballets"                           : "https://id.loc.gov/authorities/childrensSubjects/sj2021051575",
                "Bible fiction"                     : "https://id.loc.gov/authorities/genreForms/gf2014026240",
                "Bible stories"                     : "https://id.loc.gov/authorities/childrensSubjects/sj2021050145",
                "Bibliographies"                    : "https://id.loc.gov/authorities/genreForms/gf2014026048",
                "Bildungsromans"                    : "https://id.loc.gov/authorities/genreForms/gf2014026243",
                "Biographical fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026246",
                "Bowing (Musical instruments)"      : "https://id.loc.gov/authorities/subjects/sh2005003308",
                "Burlesques (Literature)"           : "https://id.loc.gov/authorities/genreForms/gf2014026254",
                "By-laws"                           : "https://id.loc.gov/authorities/genreForms/gf2011026104",
                "Catalogues raisonnés"              : "https://id.loc.gov/authorities/genreForms/gf2014026058",
                "Christian literature, Early"       : "https://id.loc.gov/authorities/subjects/sh85025115",
                "Comics (Graphic works)"            : "https://id.loc.gov/authorities/genreForms/gf2014026266",
                "Concerts"                          : "https://id.loc.gov/authorities/childrensSubjects/sj2021050694",
                "Cyberpunk fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026275",
                "Darkwawe (music)"                  : "https://id.loc.gov/authorities/genreForms/gf2014026759.html",
                "Detective and mystery fiction"     : "https://id.loc.gov/authorities/genreForms/gf2014026280",
                "Dialect fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026283",
                "Diary fiction"                     : "https://id.loc.gov/authorities/genreForms/gf2014026286",
                "Domestic fiction"                  : "https://id.loc.gov/authorities/genreForms/gf2014026295",
                "Double bass"                       : "https://id.loc.gov/authorities/childrensSubjects/sj2021060177",
                "Drama"                             : "https://id.loc.gov/authorities/childrensSubjects/sj96005303",
                "Dystopian fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026302",
                "Epic fiction"                      : "https://id.loc.gov/authorities/genreForms/gf2014026309",
                "Epistolary fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026314",
                "Erotic fiction"                    : "https://id.loc.gov/authorities/genreForms/gf2014026320",
                "Essays"                            : "https://id.loc.gov/authorities/childrensSubjects/sj2021050651",
                "Experimental fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026325",
                "Fables"                            : "https://id.loc.gov/authorities/childrensSubjects/sj2021050019",
                "Fairy tales"                       : "https://id.loc.gov/authorities/childrensSubjects/sj2021050020",
                "Fakebooks (Music)"                 : "https://id.loc.gov/authorities/genreForms/gf2014026798",
                "Fan fiction"                       : "https://id.loc.gov/authorities/childrensSubjects/sj2021061311",
                "Fantasy fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026333",
                "Flash fiction"                     : "https://id.loc.gov/authorities/genreForms/gf2014026543",
                "Folk literature"                   : "https://id.loc.gov/authorities/childrensSubjects/sj2021055518",
                "Folk tales"                        : "https://id.loc.gov/authorities/genreForms/gf2014026344",
                "Frame stories"                     : "https://id.loc.gov/authorities/genreForms/gf2014026347",
                "Gay erotic fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026320",
                "Ghost stories"                     : "https://id.loc.gov/authorities/genreForms/gf2014026357",
                "Gothic fiction"                    : "https://id.loc.gov/authorities/genreForms/gf2014026360",
                "Graphic novels"                    : "https://id.loc.gov/authorities/childrensSubjects/sj2021050898",
                "Guitar"                            : "https://id.loc.gov/authorities/childrensSubjects/sj2021052979",
                "Historical fiction"                : "https://id.loc.gov/authorities/childrensSubjects/sj2021056047",
                "Horror fiction"                    : "https://id.loc.gov/authorities/genreForms/gf2014026373",
                "Humor"                             : "https://id.loc.gov/authorities/childrensSubjects/sj2020050021",
                "Indians Music"                     : "https://id.loc.gov/authorities/subjects/sh85065058.html",
                "Legends"                           : "https://id.loc.gov/authorities/childrensSubjects/sj2021060180",
                "Library catalogs"                  : "https://id.loc.gov/authorities/genreForms/gf2015026003",
                "Literature"                        : "https://id.loc.gov/authorities/childrensSubjects/sj2021050024",
                "Magic realist fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026424",
                "Mass (Music)"                      : "https://id.loc.gov/authorities/subjects/sh85081852",
                "Masses"                            : "https://id.loc.gov/authorities/genreForms/gf2014026926",
                "Minutes (records)"                 : "https://id.loc.gov/authorities/genreForms/gf2014026128.html",
                "Motion pictures"                   : "https://id.loc.gov/authorities/childrensSubjects/sj2021051639",
                "National songs"                    : "https://id.loc.gov/authorities/childrensSubjects/sj2021058378",
                "Noir fiction"                      : "https://id.loc.gov/authorities/genreForms/gf2014026452",
                "Nonfiction novels"                 : "https://id.loc.gov/authorities/genreForms/gf2014026454",
                "Novels"                            : "https://id.loc.gov/authorities/genreForms/gf2015026020",
                "Nursery rhymes"                    : "https://id.loc.gov/authorities/childrensSubjects/sj2021050023",
                "Occational verse"                  : "https://id.loc.gov/authorities/genreForms/gf2014026460.html",
                "Octets"                            : "https://id.loc.gov/authorities/subjects/sh85093983",
                "Odes"                              : "https://id.loc.gov/authorities/genreForms/gf2014026461",
                "Organ (Musical instrument)"        : "https://id.loc.gov/authorities/childrensSubjects/sj2021058121",
                "Parodies (Literature)"             : "https://id.loc.gov/authorities/genreForms/gf2014026470",
                "Picaresque fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026479",
                "Political fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026482",
                "Psychological fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026492",
                "Puns"                              : "https://id.loc.gov/authorities/genreForms/gf2014026157",
                "Recreational works"                : "https://id.loc.gov/authorities/genreForms/gf2014026164",
                "Religious fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026502",
                "Reviews"                           : "https://id.loc.gov/authorities/genreForms/gf2014026168",
                "Robinsonades"                      : "https://id.loc.gov/authorities/genreForms/gf2014026514",
                "Romance fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026516",
                "Romance films"                     : "https://id.loc.gov/authorities/genreForms/gf2011026543",
                "Romances"                          : "https://id.loc.gov/authorities/genreForms/gf2014026517",
                "Romans à clef"                     : "https://id.loc.gov/authorities/genreForms/gf2014026518",
                "Romantic comedy films"             : "https://id.loc.gov/authorities/genreForms/gf2011026545",
                "Sagas"                             : "https://id.loc.gov/authorities/childrensSubjects/sj2021058029",
                "Satirical literature"              : "https://id.loc.gov/authorities/genreForms/gf2014026525",
                "Science fiction"                   : "https://id.loc.gov/authorities/childrensSubjects/sj2021050042",
                "Sentimental novels"                : "https://id.loc.gov/authorities/genreForms/gf2014026536",
                "Serialized fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026537",
                "Short stories"                     : "https://id.loc.gov/authorities/childrensSubjects/sj98000354",
                "Space operas"                      : "https://id.loc.gov/authorities/genreForms/gf2014026551",
                "Sprechstimme"                      : "https://id.loc.gov/authorities/subjects/sh2008000832",
                "Steampunk fiction"                 : "https://id.loc.gov/authorities/childrensSubjects/sj2021061324",
                "Stories in rhyme"                  : "https://id.loc.gov/authorities/childrensSubjects/sj2021050017",
                "Straight-edge"                     : "https://id.loc.gov/authorities/subjects/sh2003002058.html",
                "Strathspeys"                       : "https://id.loc.gov/authorities/subjects/sh85128520",
                "String octets"                     : "https://id.loc.gov/authorities/subjects/sh85129020",
                "Swamp pop music"                   : "https://id.loc.gov/authorities/genreForms/gf2014027118",
                "Symphonic poems"                   : "https://id.loc.gov/authorities/genreForms/gf2014027120",
                "Taarab"                            : "https://id.loc.gov/authorities/genreForms/gf2014027123.html",
                "Talking books"                     : "https://id.loc.gov/authorities/childrensSubjects/sj2021057301",
                "Tall tales"                        : "https://id.loc.gov/authorities/childrensSubjects/sj2021051578",
                "Tango"                             : "https://id.loc.gov/authorities/names/n96122416",
                "Te Deum laudamus"                  : "https://id.loc.gov/resources/hubs/46c964fa-238c-5179-795b-b8975adb8a34",
                "Thrillers (Fiction)"               : "https://id.loc.gov/authorities/genreForms/gf2014026571",
                "Ukulele"                           : "https://id.loc.gov/authorities/childrensSubjects/sj2021053032",
                "Utopian fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026583",
                "Violoncello"                       : "https://id.loc.gov/authorities/names/n42026083",
                "War fiction"                       : "https://id.loc.gov/authorities/genreForms/gf2014026590",
                "Western fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026594",
                "Woodwind quartets (Saxophones (4))": "https://id.loc.gov/authorities/subjects/sh85147995"]

selectByCollection('auth') { auth ->
    def data = auth.doc.data
    if (!data['@graph'][1].inScheme?.'@id'?.equals("https://id.kb.se/term/saogf")) {
        return
    }
    DocumentUtil.traverse(auth.doc.data, { value, path ->
        if (value instanceof Map && value.inScheme?.'@id'?.equals("https://id.kb.se/term/lcsh") && value.prefLabel) {
            incrementStats(value.prefLabel, path)
            auth.scheduleSave()
            return new DocumentUtil.Replace(['@id': LABEL_TO_URI[value.prefLabel]])
        }
    })
}
