// There are some 6k of these:
String where = """
id in (
 select d.id
 from lddb__versions l inner join lddb__dependencies d on l.id = d.dependsonid
 where
  l.collection = 'auth' and
  l.modified > '2023-04-17' and
  l.modified < '2023-05-31'
) and deleted = false and collection = 'bib';
"""

selectBySqlWhere(where) { data ->
    data.scheduleSave(loud: true)
}
