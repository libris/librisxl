package whelk

import java.sql.ResultSet
import java.sql.Timestamp

/**
 * Created by theodortolstoy on 2017-02-01.
 */
class VCopyDataRow {
    byte[] data
    boolean isDeleted
    Timestamp created
    Timestamp updated
    String collection
    int bib_id
    int auth_id
    byte[] authdata
    String sigel


    VCopyDataRow(ResultSet resultSet, String collection) {
        data = resultSet.getBytes('data')
        isDeleted = resultSet.getBoolean('deleted')
        created = resultSet.getTimestamp('create_date')
        updated = resultSet.getTimestamp('update_date')
        this.collection = collection
        bib_id = collection == 'bib' ? resultSet.getInt('bib_id') : 0
        auth_id = collection == 'bib' ? resultSet.getInt('auth_id') : 0
        authdata = collection == 'bib' ? resultSet.getBytes('auth_data') : null
        sigel = collection == "hold" ? resultSet.getString("shortname") : null
    }
}
