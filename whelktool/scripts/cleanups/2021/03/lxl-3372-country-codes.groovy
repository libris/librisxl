/*
Fix incorrect country codes. The codes are unfortunately tangled with one another,
witch makes the order of execution essential.
Since there is also a substitution involved, this script CAN ONLY BE RUN ONCE!

The original request, when cleaned up, looks like this (from to):
se sw
de gw
us xxu
gb xxk
nl ne
ch sz
is ic
cz xr
at au
es sp
in ii
jp ja
cn cc
il is
ro rm
ua un
ca xxc
za sa
si xv
bg bu
by bw
sk xo
au at
pt po
fo fa
ee er
br bl
ar ag
vn vm
lt li
hr ci
eg ua

The following codes are tangled: au is at ua

 */

import whelk.Document

Map <String, String> shouldBe = [:]

shouldBe["se"] = "sw"
shouldBe["de"] = "gw"
shouldBe["us"] = "xxu"
shouldBe["gb"] = "xxk"
shouldBe["nl"] = "ne"
shouldBe["ch"] = "sz"
shouldBe["is"] = "ic"
shouldBe["cz"] = "xr"
shouldBe["es"] = "sp"
shouldBe["in"] = "ii"
shouldBe["jp"] = "ja"
shouldBe["cn"] = "cc"
shouldBe["il"] = "is"
shouldBe["ro"] = "rm"
shouldBe["ua"] = "un"
shouldBe["ca"] = "xxc"
shouldBe["za"] = "sa"
shouldBe["si"] = "xv"
shouldBe["bg"] = "bu"
shouldBe["by"] = "bw"
shouldBe["sk"] = "xo"
shouldBe["pt"] = "po"
shouldBe["fo"] = "fa"
shouldBe["ee"] = "er"
shouldBe["br"] = "bl"
shouldBe["ar"] = "ag"
shouldBe["vn"] = "vm"
shouldBe["lt"] = "li"
shouldBe["hr"] = "ci"
shouldBe["eg"] = "ua"

/*
There's a pathological case (substitution), originally:
shouldBe["at"] = "au"
shouldBe["au"] = "at"
This _could_ be done naively, but it's dangerous, because if the script is interrupted while running,
there is no way to resume it. Any info about which records should have au and which should have at
is then lost.

A safer way is to do this is using a temporary value:
 */
shouldBe["at"] = "TEMP"
shouldBe["au"] = "at"
shouldBe["TEMP"] = "au"

String baseWhere = """
collection = 'bib' and
data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/SEK' and
created > '2020-04-08' and
created < '2020-04-09'
"""

// Special case requirements for each phase, to make the switches so that A -> B, B -> C doesn't lead to A -> C
List orderedPhases = [
        // must be done before eg
        """ and data#>'{@graph,1,publication}' @> '[{"@type": "PrimaryPublication", "place": [{"country": [{"@id": "https://id.kb.se/country/ua"}]}]}]'"""
        // must be done before il
        , """ and data#>'{@graph,1,publication}' @> '[{"@type": "PrimaryPublication", "place": [{"country": [{"@id": "https://id.kb.se/country/is"}]}]}]'"""
        // Triangle substitution, must be done in order
        , """ and data#>'{@graph,1,publication}' @> '[{"@type": "PrimaryPublication", "place": [{"country": [{"@id": "https://id.kb.se/country/at"}]}]}]'"""
        , """ and data#>'{@graph,1,publication}' @> '[{"@type": "PrimaryPublication", "place": [{"country": [{"@id": "https://id.kb.se/country/au"}]}]}]'"""
        // the rest are order-independent
        , ""
        ]

PrintWriter phaseLog = getReportWriter("phase-log")

for (String postfix in orderedPhases) {
    phaseLog.println("Starting phase:\n\t" + postfix)
    phaseLog.flush()
    System.err.println("Will now try\n" + baseWhere + postfix + "\n\n")
    selectBySqlWhere(baseWhere + postfix) { data ->
        boolean modified = false
        boolean holdingCriteriaFilled = false

        List<Document> holdings = data.whelk.getAttachedHoldings(data.doc.getThingIdentifiers())

        // Check the "H611:kbretro" criterium
        Document holding = holdings.find { it.getHeldBySigel() == "S"}
        if (holding && holding.data["@graph"]) {
            asList(holding.data["@graph"][1].subject).each { subj ->
                if (subj["@type"] == "Meeting" && subj["name"] == "kbretro") {
                    holdingCriteriaFilled = true
                }
            }
        }

        // Replace country code
        if (holdingCriteriaFilled) {
            String countryBase = "https://id.kb.se/country/"
            asList(data.graph[1].publication).each { publ ->
                if (publ.containsKey("country")) {
                    asList(publ.country).each{ country ->
                        String oldCountry = country["@id"].substring(countryBase.length())
                        String newCountry = shouldBe[oldCountry]
                        if (newCountry) {
                            //System.err.println("Replacing " + oldCountry + " with " + newCountry)
                            country["@id"] = countryBase + newCountry
                            modified = true
                        }
                    }
                }
            }
        }

        /*if (modified)
            data.scheduleSave()*/
    }
    phaseLog.println("Finished phase:\n\t" + postfix)
    phaseLog.flush()
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
