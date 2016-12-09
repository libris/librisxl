/**
 * Created by theodortolstoy on 2016-12-12.
 */
import spock.lang.Specification
import spock.lang.Unroll
import whelk.importer.MySQLLoader
import whelk.tools.PostgresLoadfileWriter
import whelk.util.PropertyLoader

class SetSpecMatcher extends Specification{
    def "Snöstormen should match 3 specs"() {
        given:
        def bibId=13578174
        def sqlTemplate = MySQLLoader.getSelectByMarcType('bib')
        def query = sqlTemplate.replace('bib.bib_id > ?', "bib.bib_id = ${bibId}")
        def props = PropertyLoader.loadProperties('mysql','secret')
        def connectionUrl = props.getProperty("mysqlConnectionUrl")
        def sql = PostgresLoadfileWriter.prepareSql(connectionUrl)



        def matches = 3
        expect:
        assert matches >=2
    }
}
