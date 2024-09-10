Closure normalizeTypes = script('algorithm.groovy')

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

selectByIds(['n117254ll8sv2bnm']) {
  normalizeTypes(it, loadWorkItem)
}
