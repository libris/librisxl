/*
See LXL-3739 for more info.

Manually mapped (spelling mistakes etc):

Subject headings:
Bio-bibliography => https://id.loc.gov/authorities/subjects/sh99001236 (in b8nrzx0v5cfl9tx)
Charts -> Charts, diagrams, etc. => https://id.loc.gov/authorities/subjects/sh99001425 (in ljx18cn42tlzmg1)
Land surveys => https://id.loc.gov/authorities/subjects/sh99001768 (in vs69jm9d4v5gff7)
Problems, exercises, etc. => https://id.loc.gov/authorities/subjects/sh99001553 (in sq47fd3b4wbwf38)

Genre/form:
Annals and chronicals -> Annals and chronicles => http://id.loc.gov/authorities/genreForms/gf2014026045 (in pm14cgp70q09025)
Passanger lists -> Passenger lists => https://id.loc.gov/authorities/genreForms/gf2014026138 (in 75knw0nr4bx5jvl)
Plot summeries -> Plot summaries => https://id.loc.gov/authorities/genreForms/gf2014026149 (in 75knvtgr13npl86)
Teachers’ guides -> Teachers' guides => https://id.loc.gov/authorities/genreForms/gf2014026188 (in xv8clpcg1blw9d0)
Television programs reviews -> Television program reviews => https://id.loc.gov/authorities/genreForms/gf2014026190 (in sq47gk8b2mc20mj)

TODO:
Sports programs (nl03bf463042n22):
Surveys (vs69jmvd2pts31z):

*/

import whelk.util.DocumentUtil

LABEL_TO_URI_NON_GF =
        ["Bio-bibliography"         : "https://id.loc.gov/authorities/subjects/sh99001236",
         "Charts"                   : "https://id.loc.gov/authorities/subjects/sh99001425",
         "Land surveys"             : "https://id.loc.gov/authorities/subjects/sh99001768",
         "Problems, exercises, etc.": "https://id.loc.gov/authorities/subjects/sh99001553"]

LABEL_TO_URI_GF =
        ["Annals and chronicals"              : "https://id.loc.gov/authorities/genreForms/gf2014026045",
         "Abridgements"                       : "https://id.loc.gov/authorities/genreForms/gf2014026037",
         "Abstracts"                          : "https://id.loc.gov/authorities/genreForms/gf2014026038",
         "Academic theses"                    : "https://id.loc.gov/authorities/genreForms/gf2014026039",
         "Action and adventure films"         : "https://id.loc.gov/authorities/genreForms/gf2011026005",
         "Adaptations"                        : "https://id.loc.gov/authorities/genreForms/gf2014026041",
         "Almanacs"                           : "https://id.loc.gov/authorities/genreForms/gf2014026042",
         "Amateur films"                      : "https://id.loc.gov/authorities/genreForms/gf2011026038",
         "Anagrams"                           : "https://id.loc.gov/authorities/genreForms/gf2014026043",
         "Animated films"                     : "https://id.loc.gov/authorities/genreForms/gf2011026049",
         "Annual reports"                     : "https://id.loc.gov/authorities/genreForms/gf2014026046",
         "Autobiographies"                    : "https://id.loc.gov/authorities/genreForms/gf2014026047",
         "Biographical films"                 : "https://id.loc.gov/authorities/genreForms/gf2011026089",
         "Biographies"                        : "https://id.loc.gov/authorities/genreForms/gf2014026049",
         "Blank forms"                        : "https://id.loc.gov/authorities/genreForms/gf2014026050",
         "Blogs"                              : "https://id.loc.gov/authorities/genreForms/gf2014026051",
         "Book reviews"                       : "https://id.loc.gov/authorities/genreForms/gf2014026052",
         "Braille books"                      : "https://id.loc.gov/authorities/genreForms/gf2014026053",
         "Business correspondence"            : "https://id.loc.gov/authorities/genreForms/gf2014026054",
         "Calendars"                          : "https://id.loc.gov/authorities/genreForms/gf2014026055",
         "Captivity narratives"               : "https://id.loc.gov/authorities/genreForms/gf2014026056",
         "Catalogs"                           : "https://id.loc.gov/authorities/genreForms/gf2014026057",
         "Census data"                        : "https://id.loc.gov/authorities/genreForms/gf2014026059",
         "Charades"                           : "https://id.loc.gov/authorities/genreForms/gf2014026060",
         "Children's films"                   : "https://id.loc.gov/authorities/genreForms/gf2011026123",
         "Chronologies"                       : "https://id.loc.gov/authorities/genreForms/gf2014026062",
         "Comedy films"                       : "https://id.loc.gov/authorities/genreForms/gf2011026147",
         "Commemorative works"                : "https://id.loc.gov/authorities/genreForms/gf2014026064",
         "Concert films"                      : "https://id.loc.gov/authorities/genreForms/gf2011026161",
         "Concert programs"                   : "https://id.loc.gov/authorities/genreForms/gf2014026065",
         "Concordances"                       : "https://id.loc.gov/authorities/genreForms/gf2014026066",
         "Conference materials"               : "https://id.loc.gov/authorities/genreForms/gf2014026067",
         "Conference papers and proceedings"  : "https://id.loc.gov/authorities/genreForms/gf2014026068",
         "Continuing education materials"     : "https://id.loc.gov/authorities/genreForms/gf2014026069",
         "Controlled vocabularies"            : "https://id.loc.gov/authorities/genreForms/gf2014026070",
         "Conversion tables"                  : "https://id.loc.gov/authorities/genreForms/gf2014026071",
         "Cookbooks"                          : "https://id.loc.gov/authorities/genreForms/gf2011026169",
         "Course materials"                   : "https://id.loc.gov/authorities/genreForms/gf2014026073",
         "Creative nonfiction"                : "https://id.loc.gov/authorities/genreForms/gf2014026074",
         "Crime films"                        : "https://id.loc.gov/authorities/genreForms/gf2011026177",
         "Crossword puzzles"                  : "https://id.loc.gov/authorities/genreForms/gf2014026075",
         "Cryptograms"                        : "https://id.loc.gov/authorities/genreForms/gf2014026077",
         "Dance reviews"                      : "https://id.loc.gov/authorities/genreForms/gf2014026079",
         "Databases"                          : "https://id.loc.gov/authorities/genreForms/gf2014026081",
         "Death registers"                    : "https://id.loc.gov/authorities/genreForms/gf2014026082",
         "Debates"                            : "https://id.loc.gov/authorities/genreForms/gf2014026083",
         "Derivative works"                   : "https://id.loc.gov/authorities/genreForms/gf2014026084",
         "Diaries"                            : "https://id.loc.gov/authorities/genreForms/gf2014026085",
         "Directories"                        : "https://id.loc.gov/authorities/genreForms/gf2014026087",
         "Discographies"                      : "https://id.loc.gov/authorities/genreForms/gf2014026088",
         "Documentary films"                  : "https://id.loc.gov/authorities/genreForms/gf2011026205",
         "Educational films"                  : "https://id.loc.gov/authorities/genreForms/gf2011026219",
         "Emblem books"                       : "https://id.loc.gov/authorities/genreForms/gf2014026091",
         "Encyclopedias"                      : "https://id.loc.gov/authorities/genreForms/gf2014026092",
         "Ephemera"                           : "https://id.loc.gov/authorities/genreForms/gf2014026093",
         "Ethnographic films"                 : "https://id.loc.gov/authorities/genreForms/gf2011026232",
         "Eulogies"                           : "https://id.loc.gov/authorities/genreForms/gf2014026095",
         "Examinations"                       : "https://id.loc.gov/authorities/genreForms/gf2014026096",
         "Excerpts"                           : "https://id.loc.gov/authorities/genreForms/gf2014026097",
         "Exhibition catalogs"                : "https://id.loc.gov/authorities/genreForms/gf2014026098",
         "Experimental films"                 : "https://id.loc.gov/authorities/genreForms/gf2011026235",
         "FAQs"                               : "https://id.loc.gov/authorities/genreForms/gf2014026101",
         "Facsimiles"                         : "https://id.loc.gov/authorities/genreForms/gf2014026099",
         "Family histories"                   : "https://id.loc.gov/authorities/genreForms/gf2014026100",
         "Fantasy films"                      : "https://id.loc.gov/authorities/genreForms/gf2011026242",
         "Feature films"                      : "https://id.loc.gov/authorities/genreForms/gf2011026247",
         "Film adaptations"                   : "https://id.loc.gov/authorities/genreForms/gf2011026254",
         "Film festival programs"             : "https://id.loc.gov/authorities/genreForms/gf2014026102",
         "Film trailers"                      : "https://id.loc.gov/authorities/genreForms/gf2011026263",
         "Filmed dance"                       : "https://id.loc.gov/authorities/genreForms/gf2011026266",
         "Filmed interviews"                  : "https://id.loc.gov/authorities/genreForms/gf2011026269",
         "Filmed plays"                       : "https://id.loc.gov/authorities/genreForms/gf2011026277",
         "Filmed stand-up comedy routines"    : "https://id.loc.gov/authorities/genreForms/gf2011026281",
         "Finding aids"                       : "https://id.loc.gov/authorities/genreForms/gf2014026103",
         "Genealogical tables"                : "https://id.loc.gov/authorities/genreForms/gf2014026106",
         "Guidebooks"                         : "https://id.loc.gov/authorities/genreForms/gf2014026108",
         "Haiku"                              : "https://id.loc.gov/authorities/genreForms/gf2014026366",
         "Handbooks and manuals"              : "https://id.loc.gov/authorities/genreForms/gf2014026109",
         "Historical films"                   : "https://id.loc.gov/authorities/genreForms/gf2011026311",
         "Horror films"                       : "https://id.loc.gov/authorities/genreForms/gf2011026321",
         "Illustrated works"                  : "https://id.loc.gov/authorities/genreForms/gf2014026111",
         "Informational works"                : "https://id.loc.gov/authorities/genreForms/gf2014026113",
         "Instructional and educational works": "https://id.loc.gov/authorities/genreForms/gf2014026114",
         "Instructional films"                : "https://id.loc.gov/authorities/genreForms/gf2011026333",
         "Interviews"                         : "https://id.loc.gov/authorities/genreForms/gf2014026115",
         "Job descriptions"                   : "https://id.loc.gov/authorities/genreForms/gf2014026117",
         "Laboratory manuals"                 : "https://id.loc.gov/authorities/genreForms/gf2014026120",
         "Law commentaries"                   : "https://id.loc.gov/authorities/genreForms/gf2011026150",
         "Law materials"                      : "https://id.loc.gov/authorities/genreForms/gf2011026351",
         "Lectures"                           : "https://id.loc.gov/authorities/genreForms/gf2014026122",
         "Logic puzzles"                      : "https://id.loc.gov/authorities/genreForms/gf2014026124",
         "Manufacturers' catalogs"            : "https://id.loc.gov/authorities/genreForms/gf2014026198",
         "Memorial service programs"          : "https://id.loc.gov/authorities/genreForms/gf2014026126",
         "Menus"                              : "https://id.loc.gov/authorities/genreForms/gf2014026127",
         "Motion picture reviews"             : "https://id.loc.gov/authorities/genreForms/gf2014026129",
         "Motion pictures"                    : "https://id.loc.gov/authorities/genreForms/gf2011026406",
         "Music reviews"                      : "https://id.loc.gov/authorities/genreForms/gf2014026130",
         "Music videos"                       : "https://id.loc.gov/authorities/genreForms/gf2011026413",
         "Musical films"                      : "https://id.loc.gov/authorities/genreForms/gf2011026414",
         "Nature films"                       : "https://id.loc.gov/authorities/genreForms/gf2011026416",
         "Newsletters"                        : "https://id.loc.gov/authorities/genreForms/gf2014026131",
         "Newspapers"                         : "https://id.loc.gov/authorities/genreForms/gf2014026132",
         "Newsreels"                          : "https://id.loc.gov/authorities/genreForms/gf2011026420",
         "Nonfiction films"                   : "https://id.loc.gov/authorities/genreForms/gf2011026423",
         "Notebooks"                          : "https://id.loc.gov/authorities/genreForms/gf2014026133",
         "Novellas"                           : "https://id.loc.gov/authorities/genreForms/gf2015026019",
         "Obituaries"                         : "https://id.loc.gov/authorities/genreForms/gf2014026134",
         "Opera programs"                     : "https://id.loc.gov/authorities/genreForms/gf2014026135",
         "Outlines and syllabi"               : "https://id.loc.gov/authorities/genreForms/gf2014026136",
         "Palindromes"                        : "https://id.loc.gov/authorities/genreForms/gf2014026137",
         "Passanger lists"                    : "https://id.loc.gov/authorities/genreForms/gf2014026138",
         "Patents"                            : "https://id.loc.gov/authorities/genreForms/gf2011026438",
         "Periodicals"                        : "https://id.loc.gov/authorities/genreForms/gf2014026139",
         "Perpetual calendars"                : "https://id.loc.gov/authorities/genreForms/gf2014026140",
         "Personal narratives"                : "https://id.loc.gov/authorities/genreForms/gf2014026142",
         "Photobooks"                         : "https://id.loc.gov/authorities/genreForms/gf2014026144",
         "Phrase books"                       : "https://id.loc.gov/authorities/genreForms/gf2014026145",
         "Picture dictionaries"               : "https://id.loc.gov/authorities/genreForms/gf2014026146",
         "Picture puzzles"                    : "https://id.loc.gov/authorities/genreForms/gf2014026147",
         "Plot summeries"                     : "https://id.loc.gov/authorities/genreForms/gf2014026149",
         "Poetry"                             : "https://id.loc.gov/authorities/genreForms/gf2014026481",
         "Pop-up books"                       : "https://id.loc.gov/authorities/genreForms/gf2014026150",
         "Pornographic films"                 : "https://id.loc.gov/authorities/genreForms/gf2011026460",
         "Postcards"                          : "https://id.loc.gov/authorities/genreForms/gf2014026151",
         "Press releases"                     : "https://id.loc.gov/authorities/genreForms/gf2014026153",
         "Programs (Publications)"            : "https://id.loc.gov/authorities/genreForms/gf2014026156",
         "Promotional films"                  : "https://id.loc.gov/authorities/genreForms/gf2011026470",
         "Propaganda films"                   : "https://id.loc.gov/authorities/genreForms/gf2011026472",
         "Puzzles and games"                  : "https://id.loc.gov/authorities/genreForms/gf2014026158",
         "Quotations"                         : "https://id.loc.gov/authorities/genreForms/gf2014026159",
         "Radio adaptations"                  : "https://id.loc.gov/authorities/genreForms/gf2011026484",
         "Readers (Publications)"             : "https://id.loc.gov/authorities/genreForms/gf2014026160",
         "Reality television programs"        : "https://id.loc.gov/authorities/genreForms/gf2011026522",
         "Rebuses"                            : "https://id.loc.gov/authorities/genreForms/gf2014026161",
         "Recipes"                            : "https://id.loc.gov/authorities/genreForms/gf2014026162",
         "Records (Documents)"                : "https://id.loc.gov/authorities/genreForms/gf2014026163",
         "Reference works"                    : "https://id.loc.gov/authorities/genreForms/gf2014026165",
         "Registers (Lists)"                  : "https://id.loc.gov/authorities/genreForms/gf2014026166",
         "Religious films"                    : "https://id.loc.gov/authorities/genreForms/gf2011026526",
         "Reverse indexes"                    : "https://id.loc.gov/authorities/genreForms/gf2014026167",
         "Riddles"                            : "https://id.loc.gov/authorities/genreForms/gf2014026169",
         "Sayings"                            : "https://id.loc.gov/authorities/genreForms/gf2014026170",
         "School yearbooks"                   : "https://id.loc.gov/authorities/genreForms/gf2014026172",
         "Science fiction films"              : "https://id.loc.gov/authorities/genreForms/gf2011026556",
         "Scrapbooks"                         : "https://id.loc.gov/authorities/genreForms/gf2014026173",
         "Serial publications"                : "https://id.loc.gov/authorities/genreForms/gf2014026174",
         "Short films"                        : "https://id.loc.gov/authorities/genreForms/gf2011026570",
         "Silent films"                       : "https://id.loc.gov/authorities/genreForms/gf2011026575",
         "Social problem films"               : "https://id.loc.gov/authorities/genreForms/gf2011026589",
         "Sound books"                        : "https://id.loc.gov/authorities/genreForms/gf2014026177",
         "Speeches"                           : "https://id.loc.gov/authorities/genreForms/gf2011026363",
         "Sports films"                       : "https://id.loc.gov/authorities/genreForms/gf2011026601",
         "Stand-up comedy routines"           : "https://id.loc.gov/authorities/genreForms/gf2014026180",
         "Statistics"                         : "https://id.loc.gov/authorities/genreForms/gf2014026181",
         "Study guides"                       : "https://id.loc.gov/authorities/genreForms/gf2014026182",
         "Style manuals"                      : "https://id.loc.gov/authorities/genreForms/gf2014026183",
         "Sudoku puzzles"                     : "https://id.loc.gov/authorities/genreForms/gf2014026184",
         "Tables (Data)"                      : "https://id.loc.gov/authorities/genreForms/gf2014026186",
         "Tactile works"                      : "https://id.loc.gov/authorities/genreForms/gf2014026187",
         "Teachers’ guides"                   : "https://id.loc.gov/authorities/genreForms/gf2014026188",
         "Telephone directories"              : "https://id.loc.gov/authorities/genreForms/gf2014026189",
         "Television cooking shows"           : "https://id.loc.gov/authorities/genreForms/gf2011026656",
         "Television programs reviews"        : "https://id.loc.gov/authorities/genreForms/gf2014026190",
         "Television series"                  : "https://id.loc.gov/authorities/genreForms/gf2011026680",
         "Textbooks"                          : "https://id.loc.gov/authorities/genreForms/gf2014026191",
         "Theater programs"                   : "https://id.loc.gov/authorities/genreForms/gf2014026193",
         "Theater reviews"                    : "https://id.loc.gov/authorities/genreForms/gf2014026194",
         "Thesauri (Dictionaries)"            : "https://id.loc.gov/authorities/genreForms/gf2014026195",
         "Thrillers (Motion pictures)"        : "https://id.loc.gov/authorities/genreForms/gf2011026692",
         "Toasts (Speeches)"                  : "https://id.loc.gov/authorities/genreForms/gf2014026196",
         "Toy and movable books"              : "https://id.loc.gov/authorities/genreForms/gf2014026197",
         "Tracts (Ephemera)"                  : "https://id.loc.gov/authorities/genreForms/gf2015026053",
         "Trademark lists"                    : "https://id.loc.gov/authorities/genreForms/gf2014026199",
         "Tragicomedies"                      : "https://id.loc.gov/authorities/genreForms/gf2014026577",
         "Travel writing"                     : "https://id.loc.gov/authorities/genreForms/gf2014026200",
         "Travelogues (Motion pictures)"      : "https://id.loc.gov/authorities/genreForms/gf2011026704",
         "Trivia and miscellanea"             : "https://id.loc.gov/authorities/genreForms/gf2014026201",
         "True adventure stories"             : "https://id.loc.gov/authorities/genreForms/gf2014026202",
         "True crime stories"                 : "https://id.loc.gov/authorities/genreForms/gf2014026203",
         "Union catalogs"                     : "https://id.loc.gov/authorities/genreForms/gf2014026205",
         "Video installations (Art)"          : "https://id.loc.gov/authorities/genreForms/gf2011026722",
         "Vital statistics"                   : "https://id.loc.gov/authorities/genreForms/gf2014026207",
         "War films"                          : "https://id.loc.gov/authorities/genreForms/gf2011026729",
         "Western films"                      : "https://id.loc.gov/authorities/genreForms/gf2011026735",
         "Yearbooks"                          : "https://id.loc.gov/authorities/genreForms/gf2014026208",
         "Yellow pages"                       : "https://id.loc.gov/authorities/genreForms/gf2014026209",
         "Zines"                              : "https://id.loc.gov/authorities/genreForms/gf2014026210"]

LABEL_TO_URI = LABEL_TO_URI_NON_GF + LABEL_TO_URI_GF

selectByCollection('auth') { auth ->
    def data = auth.doc.data

    if (!data['@graph'][1].inScheme?.'@id'?.equals("https://id.kb.se/term/saogf")) {
        return
    }
    DocumentUtil.traverse(auth.doc.data, { value, path ->
        if (value instanceof Map && value.inScheme?.'@id'?.equals("https://id.kb.se/term/lcgft") && value.prefLabel) {
            if (value.uri) {
                incrementStats(value.prefLabel, path)
                auth.scheduleSave()
                return new DocumentUtil.Replace(['@id': value.uri])
            } else {
                incrementStats(value.prefLabel, path)
                auth.scheduleSave()
                return new DocumentUtil.Replace(['@id': LABEL_TO_URI[value.prefLabel]])
            }
        }
    })
}
