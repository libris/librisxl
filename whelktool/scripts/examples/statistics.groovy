// See STATISTICS.txt in report dir
incrementStats('category', 'name', 'example')

selectByCollection('auth') {
    incrementStats('type', it.graph[1]['@type'])
}
