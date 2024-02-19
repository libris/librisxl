String where = "data#>>'{@graph,1,@type}' like 'ChangeObservation' and deleted = false"

selectBySqlWhere(where) {
    it.scheduleDelete()
}