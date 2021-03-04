String where = """
    collection = 'bib'
    AND data#>'{@graph,1,classification}' @> '[{\"@type\":\"marc:ClassificationNumbersAssignedInCanada\"}]'
"""

List obsoleteIndicators = [
        "marc:LcBasedCallNumberAssignedByTheContributingLibrary",
        "marc:CompleteLcClassNumberAssignedByTheContributingLibrary",
        "marc:OtherCallNumberAssignedByLac",
        "marc:IncompleteLcClassNumberAssignedByTheContributingLibrary",
        "marc:OtherCallNumberAssignedByTheContributingLibrary",
        "marc:OtherClassNumberAssignedByLac",
        "marc:OtherClassNumberAssignedByTheContributingLibrary"
]


selectBySqlWhere(where) { data ->
    List classification = data.graph[1]["classification"]

    boolean modified

    classification.each { obj ->
        modified |= obj.removeAll{ key, value ->
            key == "marc:typeCompletenessSourceOfClassCallNumber" && value in obsoleteIndicators
        }
    }

    if (modified)
        data.scheduleSave()
}
