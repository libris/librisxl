String where = """
collection = 'bib' and data#>>'{@graph,1,@type}' = 'Electronic' and
(
\tdata#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/SAH"}]' or
\tdata#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/SAHF"}]' or
\tdata#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/SAHT"}]' 
)
"""

selectBySqlWhere(where) { data ->
    Map record = data.graph[0]

    if ( ! record["inDataset"] instanceof List )
        record["inDataset"] = [record["inDataset"]]
    List datasets = record["inDataset"]
    if (!datasets.any{ it.equals( ["@id": "https://libris.kb.se/1fjdz8jnzxkc3qmw#it"] ) }) {
        datasets.add(["@id": "https://libris.kb.se/1fjdz8jnzxkc3qmw#it"])
    }

    data.scheduleSave()
}
