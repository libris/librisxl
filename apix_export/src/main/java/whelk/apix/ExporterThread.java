package whelk.apix;

import whelk.component.PostgreSQLComponent;

import java.sql.*;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExporterThread extends Thread
{
    /**
     * This atomic boolean may be toggled from outside, causing the thread to stop exporting and return
     */
    public AtomicBoolean stopAtOpportunity = new AtomicBoolean(false);

    // The "from" parameter. The exporter will export everything with a (modified) timestamp >= this value.
    private final ZonedDateTime m_exportNewerThan;

    // The "until" parameter. The exporter will export everything with a (modified) timestamp < this value.
    private final ZonedDateTime m_exportOlderThan;

    private final Properties m_properties;
    private final UI m_ui;
    private final PostgreSQLComponent m_postgreSQLComponent;

    public ExporterThread(Properties properties, ZonedDateTime exportNewerThan,
                          ZonedDateTime exportOlderThan, UI ui)
    {
        this.m_properties = properties;
        this.m_exportNewerThan = exportNewerThan;
        this.m_exportOlderThan = exportOlderThan;
        this.m_ui = ui;
        this.m_postgreSQLComponent = new PostgreSQLComponent(properties.getProperty("sqlUrl"), properties.getProperty("sqlMaintable"));
    }

    public void run()
    {
        try ( Connection connection = m_postgreSQLComponent.getConnection();
              PreparedStatement statement = prepareStatement(connection);
              ResultSet resultSet = statement.executeQuery() )
        {
            /*
            int i = 0;
            while( !stopAtOpportunity.get() )
            {
                //System.out.println("wtf, " + i);
                if ( i < 2000 )
                {
                    ++i;
                    m_ui.outputText("Pseudo call: " + m_properties.getProperty("apixUrl") + " " + i);
                }

                try {
                    sleep(100);
                }catch (InterruptedException e) {}
            }

            m_ui.outputText("Exit!");
            */
        }
        catch (SQLException e)
        {
            m_ui.outputText("Export batch stopped with SQL exception: " + e);
        }

    }

    private PreparedStatement prepareStatement(Connection connection)
            throws SQLException
    {
        String sql = "SELECT manifest, data FROM " + m_properties.getProperty("sqlMaintable") +
                " WHERE TRUE ";
        if (m_exportNewerThan != null)
            sql += "AND modified >= ? ";
        if (m_exportOlderThan != null)
            sql += "AND modified < ? ";

        int parameterIndex = 1;
        PreparedStatement statement = connection.prepareStatement(sql);
        if (m_exportNewerThan != null)
            statement.setTimestamp(parameterIndex++, new Timestamp(m_exportNewerThan.toInstant().getEpochSecond() * 1000L));
        if (m_exportOlderThan != null)
            statement.setTimestamp(parameterIndex++, new Timestamp(m_exportOlderThan.toInstant().getEpochSecond() * 1000L));

        return statement;
    }
}
