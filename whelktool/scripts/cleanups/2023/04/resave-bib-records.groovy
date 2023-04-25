def where = """
    collection = 'bib'
    and deleted = false
    and ( (modified > '2023-04-18' and modified < '2023-04-25')
        or ( (data#>>'{@graph,0,generationDate}')::date > '2023-04-18' and (data#>>'{@graph,0,generationDate}')::date < '2023-04-25' ) )
"""

selectBySqlWhere(where) {
    it.scheduleSave()
}