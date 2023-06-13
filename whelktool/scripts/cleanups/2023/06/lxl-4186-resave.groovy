// There are some 6k of these:
String where = """
id in (
 select d.id
 from lddb l inner join lddb__dependencies d on l.id = d.dependsonid
 where
  l.collection = 'auth' and
  l.modified > '2023-05-01' and
  l.modified < '2023-05-30' and
  l.changedby = 'S'
) and deleted = false;
"""

selectBySqlWhere(where) { data ->
    data.scheduleSave(loud: true)
}
