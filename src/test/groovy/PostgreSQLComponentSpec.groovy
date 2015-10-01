package whelk.component

import org.apache.commons.dbcp2.BasicDataSource
import spock.lang.Specification
import groovy.util.logging.Slf4j as Log
import whelk.Document

import java.sql.Array
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Date
import java.sql.NClob
import java.sql.ParameterMetaData
import java.sql.PreparedStatement
import java.sql.Ref
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLClientInfoException
import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.sql.Time
import java.sql.Timestamp
import java.util.concurrent.Executor


@Log
class PostgreSQLComponentSpec extends Specification {

    Storage storage


    def "should save document to database"() {
        given:
        Document doc = new Document("hej", ["@id": "hej"]).withDataset("test")
        when:
        Document r = storage.store(doc)
        then:
        r.created != null
    }


    def setup() {
        storage = new PostgreSQLComponent(null, "lddb") {
            @Override
            Connection getConnection() {
                return new Connection() {
                    @Override
                    Statement createStatement() throws SQLException {
                    }

                    @Override
                    PreparedStatement prepareStatement(String sqlString) throws SQLException {
                        return new PreparedStatement() {
                            @Override
                            ResultSet executeQuery() throws SQLException {
                                return new ResultSet() {
                                    @Override
                                    boolean next() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    void close() throws SQLException {

                                    }

                                    @Override
                                    boolean wasNull() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    String getString(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    boolean getBoolean(int columnIndex) throws SQLException {
                                        return false
                                    }

                                    @Override
                                    byte getByte(int columnIndex) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    short getShort(int columnIndex) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    int getInt(int columnIndex) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    long getLong(int columnIndex) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    float getFloat(int columnIndex) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    double getDouble(int columnIndex) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    byte[] getBytes(int columnIndex) throws SQLException {
                                        return new byte[0]
                                    }

                                    @Override
                                    Date getDate(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Time getTime(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Timestamp getTimestamp(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    InputStream getAsciiStream(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    InputStream getUnicodeStream(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    InputStream getBinaryStream(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    String getString(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    boolean getBoolean(String columnLabel) throws SQLException {
                                        return false
                                    }

                                    @Override
                                    byte getByte(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    short getShort(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    int getInt(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    long getLong(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    float getFloat(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    double getDouble(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    byte[] getBytes(String columnLabel) throws SQLException {
                                        return new byte[0]
                                    }

                                    @Override
                                    Date getDate(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Time getTime(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Timestamp getTimestamp(String columnLabel) throws SQLException {
                                        if (columnLabel == "created") {
                                            return new Timestamp(new Date().getTime())
                                        }
                                        return null
                                    }

                                    @Override
                                    InputStream getAsciiStream(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    InputStream getUnicodeStream(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    InputStream getBinaryStream(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    SQLWarning getWarnings() throws SQLException {
                                        return null
                                    }

                                    @Override
                                    void clearWarnings() throws SQLException {

                                    }

                                    @Override
                                    String getCursorName() throws SQLException {
                                        return null
                                    }

                                    @Override
                                    ResultSetMetaData getMetaData() throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Object getObject(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Object getObject(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    int findColumn(String columnLabel) throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    Reader getCharacterStream(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Reader getCharacterStream(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    BigDecimal getBigDecimal(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    BigDecimal getBigDecimal(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    boolean isBeforeFirst() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean isAfterLast() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean isFirst() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean isLast() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    void beforeFirst() throws SQLException {

                                    }

                                    @Override
                                    void afterLast() throws SQLException {

                                    }

                                    @Override
                                    boolean first() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean last() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    int getRow() throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    boolean absolute(int row) throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean relative(int rows) throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean previous() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    void setFetchDirection(int direction) throws SQLException {

                                    }

                                    @Override
                                    int getFetchDirection() throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    void setFetchSize(int rows) throws SQLException {

                                    }

                                    @Override
                                    int getFetchSize() throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    int getType() throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    int getConcurrency() throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    boolean rowUpdated() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean rowInserted() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    boolean rowDeleted() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    void updateNull(int columnIndex) throws SQLException {

                                    }

                                    @Override
                                    void updateBoolean(int columnIndex, boolean x) throws SQLException {

                                    }

                                    @Override
                                    void updateByte(int columnIndex, byte x) throws SQLException {

                                    }

                                    @Override
                                    void updateShort(int columnIndex, short x) throws SQLException {

                                    }

                                    @Override
                                    void updateInt(int columnIndex, int x) throws SQLException {

                                    }

                                    @Override
                                    void updateLong(int columnIndex, long x) throws SQLException {

                                    }

                                    @Override
                                    void updateFloat(int columnIndex, float x) throws SQLException {

                                    }

                                    @Override
                                    void updateDouble(int columnIndex, double x) throws SQLException {

                                    }

                                    @Override
                                    void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

                                    }

                                    @Override
                                    void updateString(int columnIndex, String x) throws SQLException {

                                    }

                                    @Override
                                    void updateBytes(int columnIndex, byte[] x) throws SQLException {

                                    }

                                    @Override
                                    void updateDate(int columnIndex, Date x) throws SQLException {

                                    }

                                    @Override
                                    void updateTime(int columnIndex, Time x) throws SQLException {

                                    }

                                    @Override
                                    void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

                                    }

                                    @Override
                                    void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

                                    }

                                    @Override
                                    void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

                                    }

                                    @Override
                                    void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

                                    }

                                    @Override
                                    void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

                                    }

                                    @Override
                                    void updateObject(int columnIndex, Object x) throws SQLException {

                                    }

                                    @Override
                                    void updateNull(String columnLabel) throws SQLException {

                                    }

                                    @Override
                                    void updateBoolean(String columnLabel, boolean x) throws SQLException {

                                    }

                                    @Override
                                    void updateByte(String columnLabel, byte x) throws SQLException {

                                    }

                                    @Override
                                    void updateShort(String columnLabel, short x) throws SQLException {

                                    }

                                    @Override
                                    void updateInt(String columnLabel, int x) throws SQLException {

                                    }

                                    @Override
                                    void updateLong(String columnLabel, long x) throws SQLException {

                                    }

                                    @Override
                                    void updateFloat(String columnLabel, float x) throws SQLException {

                                    }

                                    @Override
                                    void updateDouble(String columnLabel, double x) throws SQLException {

                                    }

                                    @Override
                                    void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

                                    }

                                    @Override
                                    void updateString(String columnLabel, String x) throws SQLException {

                                    }

                                    @Override
                                    void updateBytes(String columnLabel, byte[] x) throws SQLException {

                                    }

                                    @Override
                                    void updateDate(String columnLabel, Date x) throws SQLException {

                                    }

                                    @Override
                                    void updateTime(String columnLabel, Time x) throws SQLException {

                                    }

                                    @Override
                                    void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

                                    }

                                    @Override
                                    void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

                                    }

                                    @Override
                                    void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

                                    }

                                    @Override
                                    void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

                                    }

                                    @Override
                                    void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

                                    }

                                    @Override
                                    void updateObject(String columnLabel, Object x) throws SQLException {

                                    }

                                    @Override
                                    void insertRow() throws SQLException {

                                    }

                                    @Override
                                    void updateRow() throws SQLException {

                                    }

                                    @Override
                                    void deleteRow() throws SQLException {

                                    }

                                    @Override
                                    void refreshRow() throws SQLException {

                                    }

                                    @Override
                                    void cancelRowUpdates() throws SQLException {

                                    }

                                    @Override
                                    void moveToInsertRow() throws SQLException {

                                    }

                                    @Override
                                    void moveToCurrentRow() throws SQLException {

                                    }

                                    @Override
                                    Statement getStatement() throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Ref getRef(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Blob getBlob(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Clob getClob(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Array getArray(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Ref getRef(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Blob getBlob(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Clob getClob(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Array getArray(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Date getDate(int columnIndex, Calendar cal) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Date getDate(String columnLabel, Calendar cal) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Time getTime(int columnIndex, Calendar cal) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Time getTime(String columnLabel, Calendar cal) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    URL getURL(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    URL getURL(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    void updateRef(int columnIndex, Ref x) throws SQLException {

                                    }

                                    @Override
                                    void updateRef(String columnLabel, Ref x) throws SQLException {

                                    }

                                    @Override
                                    void updateBlob(int columnIndex, Blob x) throws SQLException {

                                    }

                                    @Override
                                    void updateBlob(String columnLabel, Blob x) throws SQLException {

                                    }

                                    @Override
                                    void updateClob(int columnIndex, Clob x) throws SQLException {

                                    }

                                    @Override
                                    void updateClob(String columnLabel, Clob x) throws SQLException {

                                    }

                                    @Override
                                    void updateArray(int columnIndex, Array x) throws SQLException {

                                    }

                                    @Override
                                    void updateArray(String columnLabel, Array x) throws SQLException {

                                    }

                                    @Override
                                    RowId getRowId(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    RowId getRowId(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    void updateRowId(int columnIndex, RowId x) throws SQLException {

                                    }

                                    @Override
                                    void updateRowId(String columnLabel, RowId x) throws SQLException {

                                    }

                                    @Override
                                    int getHoldability() throws SQLException {
                                        return 0
                                    }

                                    @Override
                                    boolean isClosed() throws SQLException {
                                        return false
                                    }

                                    @Override
                                    void updateNString(int columnIndex, String nString) throws SQLException {

                                    }

                                    @Override
                                    void updateNString(String columnLabel, String nString) throws SQLException {

                                    }

                                    @Override
                                    void updateNClob(int columnIndex, NClob nClob) throws SQLException {

                                    }

                                    @Override
                                    void updateNClob(String columnLabel, NClob nClob) throws SQLException {

                                    }

                                    @Override
                                    NClob getNClob(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    NClob getNClob(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    SQLXML getSQLXML(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    SQLXML getSQLXML(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

                                    }

                                    @Override
                                    void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

                                    }

                                    @Override
                                    String getNString(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    String getNString(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Reader getNCharacterStream(int columnIndex) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    Reader getNCharacterStream(String columnLabel) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

                                    }

                                    @Override
                                    void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

                                    }

                                    @Override
                                    void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

                                    }

                                    @Override
                                    void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

                                    }

                                    @Override
                                    void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

                                    }

                                    @Override
                                    void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

                                    }

                                    @Override
                                    void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

                                    }

                                    @Override
                                    void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

                                    }

                                    @Override
                                    void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

                                    }

                                    @Override
                                    void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

                                    }

                                    @Override
                                    void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

                                    }

                                    @Override
                                    void updateClob(int columnIndex, Reader reader) throws SQLException {

                                    }

                                    @Override
                                    void updateClob(String columnLabel, Reader reader) throws SQLException {

                                    }

                                    @Override
                                    void updateNClob(int columnIndex, Reader reader) throws SQLException {

                                    }

                                    @Override
                                    void updateNClob(String columnLabel, Reader reader) throws SQLException {

                                    }

                                    @Override
                                    def <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    def <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    def <T> T unwrap(Class<T> iface) throws SQLException {
                                        return null
                                    }

                                    @Override
                                    boolean isWrapperFor(Class<?> iface) throws SQLException {
                                        return false
                                    }
                                }
                            }

                            @Override
                            int executeUpdate() throws SQLException {
                                return 1
                            }

                            @Override
                            void setNull(int parameterIndex, int sqlType) throws SQLException {

                            }

                            @Override
                            void setBoolean(int parameterIndex, boolean x) throws SQLException {

                            }

                            @Override
                            void setByte(int parameterIndex, byte x) throws SQLException {

                            }

                            @Override
                            void setShort(int parameterIndex, short x) throws SQLException {

                            }

                            @Override
                            void setInt(int parameterIndex, int x) throws SQLException {

                            }

                            @Override
                            void setLong(int parameterIndex, long x) throws SQLException {

                            }

                            @Override
                            void setFloat(int parameterIndex, float x) throws SQLException {

                            }

                            @Override
                            void setDouble(int parameterIndex, double x) throws SQLException {

                            }

                            @Override
                            void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

                            }

                            @Override
                            void setString(int parameterIndex, String x) throws SQLException {

                            }

                            @Override
                            void setBytes(int parameterIndex, byte[] x) throws SQLException {

                            }

                            @Override
                            void setDate(int parameterIndex, Date x) throws SQLException {

                            }

                            @Override
                            void setTime(int parameterIndex, Time x) throws SQLException {

                            }

                            @Override
                            void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

                            }

                            @Override
                            void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

                            }

                            @Override
                            void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

                            }

                            @Override
                            void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

                            }

                            @Override
                            void clearParameters() throws SQLException {

                            }

                            @Override
                            void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

                            }

                            @Override
                            void setObject(int parameterIndex, Object x) throws SQLException {

                            }

                            @Override
                            boolean execute() throws SQLException {
                                return false
                            }

                            @Override
                            void addBatch() throws SQLException {

                            }

                            @Override
                            void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

                            }

                            @Override
                            void setRef(int parameterIndex, Ref x) throws SQLException {

                            }

                            @Override
                            void setBlob(int parameterIndex, Blob x) throws SQLException {

                            }

                            @Override
                            void setClob(int parameterIndex, Clob x) throws SQLException {

                            }

                            @Override
                            void setArray(int parameterIndex, Array x) throws SQLException {

                            }

                            @Override
                            ResultSetMetaData getMetaData() throws SQLException {
                                return null
                            }

                            @Override
                            void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

                            }

                            @Override
                            void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

                            }

                            @Override
                            void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

                            }

                            @Override
                            void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

                            }

                            @Override
                            void setURL(int parameterIndex, URL x) throws SQLException {

                            }

                            @Override
                            ParameterMetaData getParameterMetaData() throws SQLException {
                                return null
                            }

                            @Override
                            void setRowId(int parameterIndex, RowId x) throws SQLException {

                            }

                            @Override
                            void setNString(int parameterIndex, String value) throws SQLException {

                            }

                            @Override
                            void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

                            }

                            @Override
                            void setNClob(int parameterIndex, NClob value) throws SQLException {

                            }

                            @Override
                            void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

                            }

                            @Override
                            void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

                            }

                            @Override
                            void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

                            }

                            @Override
                            void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

                            }

                            @Override
                            void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

                            }

                            @Override
                            void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

                            }

                            @Override
                            void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

                            }

                            @Override
                            void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

                            }

                            @Override
                            void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

                            }

                            @Override
                            void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

                            }

                            @Override
                            void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

                            }

                            @Override
                            void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

                            }

                            @Override
                            void setClob(int parameterIndex, Reader reader) throws SQLException {

                            }

                            @Override
                            void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

                            }

                            @Override
                            void setNClob(int parameterIndex, Reader reader) throws SQLException {

                            }

                            @Override
                            ResultSet executeQuery(String sql) throws SQLException {
                                return null
                            }

                            @Override
                            int executeUpdate(String sql) throws SQLException {
                                return 0
                            }

                            @Override
                            void close() throws SQLException {

                            }

                            @Override
                            int getMaxFieldSize() throws SQLException {
                                return 0
                            }

                            @Override
                            void setMaxFieldSize(int max) throws SQLException {

                            }

                            @Override
                            int getMaxRows() throws SQLException {
                                return 0
                            }

                            @Override
                            void setMaxRows(int max) throws SQLException {

                            }

                            @Override
                            void setEscapeProcessing(boolean enable) throws SQLException {

                            }

                            @Override
                            int getQueryTimeout() throws SQLException {
                                return 0
                            }

                            @Override
                            void setQueryTimeout(int seconds) throws SQLException {

                            }

                            @Override
                            void cancel() throws SQLException {

                            }

                            @Override
                            SQLWarning getWarnings() throws SQLException {
                                return null
                            }

                            @Override
                            void clearWarnings() throws SQLException {

                            }

                            @Override
                            void setCursorName(String name) throws SQLException {

                            }

                            @Override
                            boolean execute(String sql) throws SQLException {
                                return false
                            }

                            @Override
                            ResultSet getResultSet() throws SQLException {
                                return null
                            }

                            @Override
                            int getUpdateCount() throws SQLException {
                                return 0
                            }

                            @Override
                            boolean getMoreResults() throws SQLException {
                                return false
                            }

                            @Override
                            void setFetchDirection(int direction) throws SQLException {

                            }

                            @Override
                            int getFetchDirection() throws SQLException {
                                return 0
                            }

                            @Override
                            void setFetchSize(int rows) throws SQLException {

                            }

                            @Override
                            int getFetchSize() throws SQLException {
                                return 0
                            }

                            @Override
                            int getResultSetConcurrency() throws SQLException {
                                return 0
                            }

                            @Override
                            int getResultSetType() throws SQLException {
                                return 0
                            }

                            @Override
                            void addBatch(String sql) throws SQLException {

                            }

                            @Override
                            void clearBatch() throws SQLException {

                            }

                            @Override
                            int[] executeBatch() throws SQLException {
                                return new int[0]
                            }

                            @Override
                            Connection getConnection() throws SQLException {
                                return null
                            }

                            @Override
                            boolean getMoreResults(int current) throws SQLException {
                                return false
                            }

                            @Override
                            ResultSet getGeneratedKeys() throws SQLException {
                                return null
                            }

                            @Override
                            int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
                                return 0
                            }

                            @Override
                            int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
                                return 0
                            }

                            @Override
                            int executeUpdate(String sql, String[] columnNames) throws SQLException {
                                return 0
                            }

                            @Override
                            boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
                                return false
                            }

                            @Override
                            boolean execute(String sql, int[] columnIndexes) throws SQLException {
                                return false
                            }

                            @Override
                            boolean execute(String sql, String[] columnNames) throws SQLException {
                                return false
                            }

                            @Override
                            int getResultSetHoldability() throws SQLException {
                                return 0
                            }

                            @Override
                            boolean isClosed() throws SQLException {
                                return false
                            }

                            @Override
                            void setPoolable(boolean poolable) throws SQLException {

                            }

                            @Override
                            boolean isPoolable() throws SQLException {
                                return false
                            }

                            @Override
                            void closeOnCompletion() throws SQLException {

                            }

                            @Override
                            boolean isCloseOnCompletion() throws SQLException {
                                return false
                            }

                            @Override
                            def <T> T unwrap(Class<T> iface) throws SQLException {
                                return null
                            }

                            @Override
                            boolean isWrapperFor(Class<?> iface) throws SQLException {
                                return false
                            }
                        }
                    }

                    @Override
                    CallableStatement prepareCall(String sql) throws SQLException {
                        return null
                    }

                    @Override
                    String nativeSQL(String sql) throws SQLException {
                        return null
                    }

                    @Override
                    void setAutoCommit(boolean autoCommit) throws SQLException {

                    }

                    @Override
                    boolean getAutoCommit() throws SQLException {
                        return false
                    }

                    @Override
                    void commit() throws SQLException {

                    }

                    @Override
                    void rollback() throws SQLException {

                    }

                    @Override
                    void close() throws SQLException {

                    }

                    @Override
                    boolean isClosed() throws SQLException {
                        return false
                    }

                    @Override
                    DatabaseMetaData getMetaData() throws SQLException {
                        return null
                    }

                    @Override
                    void setReadOnly(boolean readOnly) throws SQLException {

                    }

                    @Override
                    boolean isReadOnly() throws SQLException {
                        return false
                    }

                    @Override
                    void setCatalog(String catalog) throws SQLException {

                    }

                    @Override
                    String getCatalog() throws SQLException {
                        return null
                    }

                    @Override
                    void setTransactionIsolation(int level) throws SQLException {

                    }

                    @Override
                    int getTransactionIsolation() throws SQLException {
                        return 0
                    }

                    @Override
                    SQLWarning getWarnings() throws SQLException {
                        return null
                    }

                    @Override
                    void clearWarnings() throws SQLException {

                    }

                    @Override
                    Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
                        return null
                    }

                    @Override
                    PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
                        return null
                    }

                    @Override
                    CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
                        return null
                    }

                    @Override
                    Map<String, Class<?>> getTypeMap() throws SQLException {
                        return null
                    }

                    @Override
                    void setTypeMap(Map<String, Class<?>> map) throws SQLException {

                    }

                    @Override
                    void setHoldability(int holdability) throws SQLException {

                    }

                    @Override
                    int getHoldability() throws SQLException {
                        return 0
                    }

                    @Override
                    Savepoint setSavepoint() throws SQLException {
                        return null
                    }

                    @Override
                    Savepoint setSavepoint(String name) throws SQLException {
                        return null
                    }

                    @Override
                    void rollback(Savepoint savepoint) throws SQLException {

                    }

                    @Override
                    void releaseSavepoint(Savepoint savepoint) throws SQLException {

                    }

                    @Override
                    Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
                        return null
                    }

                    @Override
                    PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
                        return null
                    }

                    @Override
                    CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
                        return null
                    }

                    @Override
                    PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
                        return null
                    }

                    @Override
                    PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
                        return null
                    }

                    @Override
                    PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
                        return null
                    }

                    @Override
                    Clob createClob() throws SQLException {
                        return null
                    }

                    @Override
                    Blob createBlob() throws SQLException {
                        return null
                    }

                    @Override
                    NClob createNClob() throws SQLException {
                        return null
                    }

                    @Override
                    SQLXML createSQLXML() throws SQLException {
                        return null
                    }

                    @Override
                    boolean isValid(int timeout) throws SQLException {
                        return false
                    }

                    @Override
                    void setClientInfo(String name, String value) throws SQLClientInfoException {

                    }

                    @Override
                    void setClientInfo(Properties properties) throws SQLClientInfoException {

                    }

                    @Override
                    String getClientInfo(String name) throws SQLException {
                        return null
                    }

                    @Override
                    Properties getClientInfo() throws SQLException {
                        return null
                    }

                    @Override
                    Array createArrayOf(String typeName, Object[] elements) throws SQLException {
                        return null
                    }

                    @Override
                    Struct createStruct(String typeName, Object[] attributes) throws SQLException {
                        return null
                    }

                    @Override
                    void setSchema(String schema) throws SQLException {

                    }

                    @Override
                    String getSchema() throws SQLException {
                        return null
                    }

                    @Override
                    void abort(Executor executor) throws SQLException {

                    }

                    @Override
                    void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

                    }

                    @Override
                    int getNetworkTimeout() throws SQLException {
                        return 0
                    }

                    @Override
                    def <T> T unwrap(Class<T> iface) throws SQLException {
                        return null
                    }

                    @Override
                    boolean isWrapperFor(Class<?> iface) throws SQLException {
                        return false
                    }
                }
            }
        }
    }

}
