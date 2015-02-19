package whelk.importer

import whelk.plugin.BasicPlugin
import whelk.plugin.Importer

import java.sql.Connection
import java.sql.PreparedStatement

/**
 * Created by markus on 19/02/15.
 */
abstract class MySQLImporter extends BasicPlugin implements Importer {

    abstract PreparedStatement prepareStatement(String dataset, Connection connection);

}
