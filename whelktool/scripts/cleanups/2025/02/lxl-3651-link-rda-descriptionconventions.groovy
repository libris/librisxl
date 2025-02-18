/**
 * Replace local descriptionConventions code:rda with link
 *
 * See https://kbse.atlassian.net/browse/LXL-3651
 */

var process = { doc ->
    List rda = asList(doc.graph[0]["descriptionConventions"]).findAll { it.code == "rda" }

    rda.forEach {
        // fix manual errors in type, e.g. marc:CatalogingRulesType
        if (it["@type"] != "DescriptionConventions") {
            incrementStats("@type", it["@type"] ? it["@type"] : "null")

            it["@type"] = "DescriptionConventions"
        }
    }

    // heuristicIdentity -> automatically linked based on code on save
    if (!rda.isEmpty()) {
        doc.scheduleSave()
    }
}

selectByCollection("auth", process)
selectByCollection("bib", process)