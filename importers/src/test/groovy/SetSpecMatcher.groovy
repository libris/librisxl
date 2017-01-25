/**
 * Created by theodortolstoy on 2016-12-12.
 * Tests for asserting that bib-auth linking works. TODO: remove database dependencies.
 */


import spock.lang.Specification
import whelk.importer.MySQLLoader
import whelk.tools.MarcFrameConvertingActor
import whelk.tools.PostgresLoadfileWriter
import whelk.tools.VCopyDataRow
import whelk.util.PropertyLoader

import java.sql.ResultSet

class SetSpecMatcher extends Specification {

    Map getDataRowsFromBibId(int bibId) {
        def convertingActor = new MarcFrameConvertingActor()
        convertingActor.start()
        def collection = "bib"
        def sqlTemplate = MySQLLoader.selectByMarcType[collection]
        def query = sqlTemplate.replace('bib.bib_id > ?', "bib.bib_id = ${bibId}")
        def props = PropertyLoader.loadProperties('mysql', 'secret')
        def connectionUrl = props.getProperty("mysqlConnectionUrl")
        def sql = PostgresLoadfileWriter.prepareSql(connectionUrl)
        def rows = []
        sql.eachRow(query) { ResultSet it ->
            rows.add(new VCopyDataRow(it, collection))
        }
        PostgresLoadfileWriter.handleRowGroup(rows, convertingActor)

    }

    def "13578174 should match 2 specs"() { //Three in due time...
        given:
        def bibId = 13578174
        def docAndSpecs = getDataRowsFromBibId(bibId)
        expect:
        //Interim assertion. Final assert should point to actual auth record and contain three auth records
        docAndSpecs.document.data.'@graph'[0].'_marcUncompleted'.findAll { it ->
            it.'_unhandled'.any {
                it == '0'
            }
        }.size() == 2
    }

    def "11525453 should match 1 spec"() {
        given:
        def bibId = 11525453
        def docAndSpecs = getDataRowsFromBibId(bibId)
        expect:
        //TODO: make this assertion less brutal...
        docAndSpecs.document.data.inspect().contains('http://libris.kb.se/resource/auth/347764')
    }
}
