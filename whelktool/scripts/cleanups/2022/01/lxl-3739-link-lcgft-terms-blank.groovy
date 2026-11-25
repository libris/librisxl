/*
See LXL-3739 for more info.

URI for <label> obtained from location response header of "curl -I -o headers -s ${url}"
where url="https://id.loc.gov/authorities/genreForm/label/<label>"

Manually edited (spelling mistakes etc):

Darkwawe (music) -> Darkwave (Music) (in pm14bfp71ldshtn)
Taraab -> Taraab (Music) (in wt7bjpmf1s02t5n)
Occational verse -> Occasional verse (in khw087h30db2179)
Alternative histories (fiction) -> Alternative histories (Fiction) (in 0xbdhl4j3g28rlz)
Indians Music -> Indians--Music (in 86lpwfvs37m8dj2)
Minutes (records) -> Minutes (Records) (in 0xbfnmfj240rck1)
Tango -> Tangos (Music) (in 97mqx3jt059lds6)

Terms not available at https://id.loc.gov/authorities/genreForm/label were checked manually with the LC
suggest API https://id.loc.gov/suggest2?q=<label>.
*/

import whelk.util.DocumentUtil

LABEL_TO_URI_NON_GF = ["Indians Music" : "https://id.loc.gov/authorities/subjects/sh85065058"]
LABEL_TO_URI_GF =
        ["Action and adventure fiction"   : "https://id.loc.gov/authorities/genreForms/gf2014026217",
         "Administrative regulations"     : "https://id.loc.gov/authorities/genreForms/gf2011026030",
         "Allegories"                     : "https://id.loc.gov/authorities/genreForms/gf2014026218",
         "Alternative histories (fiction)": "https://id.loc.gov/authorities/genreForms/gf2014026220",
         "Arthurian romances"             : "https://id.loc.gov/authorities/genreForms/gf2014026227",
         "Audiobooks"                     : "https://id.loc.gov/authorities/genreForms/gf2011026063",
         "Autobiographical fiction"       : "https://id.loc.gov/authorities/genreForms/gf2014026231",
         "Bible fiction"                  : "https://id.loc.gov/authorities/genreForms/gf2014026240",
         "Bible stories"                  : "https://id.loc.gov/authorities/genreForms/gf2014026242",
         "Bibliographies"                 : "https://id.loc.gov/authorities/genreForms/gf2014026048",
         "Bildungsromans"                 : "https://id.loc.gov/authorities/genreForms/gf2014026243",
         "Biographical fiction"           : "https://id.loc.gov/authorities/genreForms/gf2014026246",
         "Burlesques (Literature)"        : "https://id.loc.gov/authorities/genreForms/gf2014026254",
         "By-laws"                        : "https://id.loc.gov/authorities/genreForms/gf2011026104",
         "Catalogues raisonnés"           : "https://id.loc.gov/authorities/genreForms/gf2014026058",
         "Comics (Graphic works)"         : "https://id.loc.gov/authorities/genreForms/gf2014026266",
         "Cyberpunk fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026275",
         "Darkwawe (music)"               : "https://id.loc.gov/authorities/genreForms/gf2014026759",
         "Detective and mystery fiction"  : "https://id.loc.gov/authorities/genreForms/gf2014026280",
         "Dialect fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026283",
         "Diary fiction"                  : "https://id.loc.gov/authorities/genreForms/gf2014026286",
         "Domestic fiction"               : "https://id.loc.gov/authorities/genreForms/gf2014026295",
         "Drama"                          : "https://id.loc.gov/authorities/genreForms/gf2014026297",
         "Dystopian fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026302",
         "Epic fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026309",
         "Epistolary fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026314",
         "Erotic fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026320",
         "Essays"                         : "https://id.loc.gov/authorities/genreForms/gf2014026094",
         "Experimental fiction"           : "https://id.loc.gov/authorities/genreForms/gf2014026325",
         "Fables"                         : "https://id.loc.gov/authorities/genreForms/gf2014026327",
         "Fairy tales"                    : "https://id.loc.gov/authorities/genreForms/gf2014026329",
         "Fakebooks (Music)"              : "https://id.loc.gov/authorities/genreForms/gf2014026798",
         "Fan fiction"                    : "https://id.loc.gov/authorities/genreForms/gf2014026330",
         "Fantasy fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026333",
         "Flash fiction"                  : "https://id.loc.gov/authorities/genreForms/gf2014026543",
         "Folk literature"                : "https://id.loc.gov/authorities/genreForms/gf2014026342",
         "Folk tales"                     : "https://id.loc.gov/authorities/genreForms/gf2014026344",
         "Frame stories"                  : "https://id.loc.gov/authorities/genreForms/gf2014026347",
         "Gay erotic fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026320",
         "Ghost stories"                  : "https://id.loc.gov/authorities/genreForms/gf2014026357",
         "Gothic fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026360",
         "Graphic novels"                 : "https://id.loc.gov/authorities/genreForms/gf2014026362",
         "Historical fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026370",
         "Horror fiction"                 : "https://id.loc.gov/authorities/genreForms/gf2014026373",
         "Humor"                          : "https://id.loc.gov/authorities/genreForms/gf2014026110",
         "Legends"                        : "https://id.loc.gov/authorities/genreForms/gf2014026407",
         "Library catalogs"               : "https://id.loc.gov/authorities/genreForms/gf2015026003",
         "Literature"                     : "https://id.loc.gov/authorities/genreForms/gf2014026415",
         "Magic realist fiction"          : "https://id.loc.gov/authorities/genreForms/gf2014026424",
         "Masses"                         : "https://id.loc.gov/authorities/genreForms/gf2014026926",
         "Minutes (records)"              : "https://id.loc.gov/authorities/genreForms/gf2014026128",
         "Motion pictures"                : "https://id.loc.gov/authorities/genreForms/gf2011026406",
         "Noir fiction"                   : "https://id.loc.gov/authorities/genreForms/gf2014026452",
         "Nonfiction novels"              : "https://id.loc.gov/authorities/genreForms/gf2014026454",
         "Novels"                         : "https://id.loc.gov/authorities/genreForms/gf2015026020",
         "Nursery rhymes"                 : "https://id.loc.gov/authorities/genreForms/gf2014026459",
         "Occational verse"               : "https://id.loc.gov/authorities/genreForms/gf2014026460",
         "Odes"                           : "https://id.loc.gov/authorities/genreForms/gf2014026461",
         "Parodies (Literature)"          : "https://id.loc.gov/authorities/genreForms/gf2014026470",
         "Picaresque fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026479",
         "Political fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026482",
         "Psychological fiction"          : "https://id.loc.gov/authorities/genreForms/gf2014026492",
         "Puns"                           : "https://id.loc.gov/authorities/genreForms/gf2014026157",
         "Recreational works"             : "https://id.loc.gov/authorities/genreForms/gf2014026164",
         "Religious fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026502",
         "Reviews"                        : "https://id.loc.gov/authorities/genreForms/gf2014026168",
         "Robinsonades"                   : "https://id.loc.gov/authorities/genreForms/gf2014026514",
         "Romance fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026516",
         "Romance films"                  : "https://id.loc.gov/authorities/genreForms/gf2011026543",
         "Romances"                       : "https://id.loc.gov/authorities/genreForms/gf2014026517",
         "Romans à clef"                  : "https://id.loc.gov/authorities/genreForms/gf2014026518",
         "Romantic comedy films"          : "https://id.loc.gov/authorities/genreForms/gf2011026545",
         "Sagas"                          : "https://id.loc.gov/authorities/genreForms/gf2014026522",
         "Satirical literature"           : "https://id.loc.gov/authorities/genreForms/gf2014026525",
         "Science fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026529",
         "Sentimental novels"             : "https://id.loc.gov/authorities/genreForms/gf2014026536",
         "Serialized fiction"             : "https://id.loc.gov/authorities/genreForms/gf2014026537",
         "Short stories"                  : "https://id.loc.gov/authorities/genreForms/gf2014026542",
         "Space operas"                   : "https://id.loc.gov/authorities/genreForms/gf2014026551",
         "Steampunk fiction"              : "https://id.loc.gov/authorities/genreForms/gf2014026558",
         "Stories in rhyme"               : "https://id.loc.gov/authorities/genreForms/gf2014026559",
         "Strathspeys"                    : "https://id.loc.gov/authorities/genreForms/gf2014027114",
         "Swamp pop music"                : "https://id.loc.gov/authorities/genreForms/gf2014027118",
         "Symphonic poems"                : "https://id.loc.gov/authorities/genreForms/gf2014027120",
         "Taarab"                         : "https://id.loc.gov/authorities/genreForms/gf2014027123",
         "Talking books"                  : "https://id.loc.gov/authorities/genreForms/gf2011026630",
         "Tall tales"                     : "https://id.loc.gov/authorities/genreForms/gf2014026565",
         "Tango"                          : "https://id.loc.gov/authorities/genreForms/gf2014027127",
         "Thrillers (Fiction)"            : "https://id.loc.gov/authorities/genreForms/gf2014026571",
         "Utopian fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026583",
         "War fiction"                    : "https://id.loc.gov/authorities/genreForms/gf2014026590",
         "Western fiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026594"]

LABEL_TO_URI = LABEL_TO_URI_GF + LABEL_TO_URI_NON_GF

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
