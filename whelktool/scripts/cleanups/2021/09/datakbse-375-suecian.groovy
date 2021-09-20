String where = """
collection = 'bib' and  
(
\tdata#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/SAH"}]' or
\tdata#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/SAHF"}]' or
\tdata#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/SAHT"}]' 
) and not data#>'{@graph,0,inDataset}' @> '[{"@id": "https://libris.kb.se/1fjdz8jnzxkc3qmw#it"}]'
"""

selectBySqlWhere(where) { data ->
    Map record = data.graph[0]

    if ( ! record["inDataset"] instanceof List )
        record["inDataset"] = [record["inDataset"]]
    record["inDataset"].add(["@id": "https://libris.kb.se/1fjdz8jnzxkc3qmw#it"])

    data.scheduleSave()
}
