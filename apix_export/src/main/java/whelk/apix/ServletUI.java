package whelk.apix;

import com.github.jsonldjava.utils.Obj;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.component.PostgreSQLComponent;
import whelk.util.PropertyLoader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static whelk.component.PostgreSQLComponent.mapper;

public class ServletUI extends HttpServlet implements UI
{
    final static int PSEUDO_CONSOLE_LINES = 64;
    final String LDDB_ROW_KEY_NAME = "apix_exporter";
    final String LDDB_TIMESTAMP_KEY_NAME = "lastTimestamp";

    ExporterThread m_exporterThread = null;
    String[] m_pseudoConsole = new String[PSEUDO_CONSOLE_LINES];
    int m_pseudoConsoleNext = 0;
    Properties m_properties = null;
    PostgreSQLComponent m_postgreSQLComponent;

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException
    {
        String path = req.getPathInfo();
        if (path == null)
        {
            res.sendError(404);
            return;
        }

        res.setContentType("text/plain");

        switch (path)
        {
            case "/console":
                res.getOutputStream().print( getPseudoConsole() );
                break;
            case "/endpoint":
                res.getOutputStream().print( m_properties.getProperty("apixHost") );
                break;
            case "/endpointdb":
                res.getOutputStream().print( m_properties.getProperty("apixDatabase") );
                break;
            case "/startpoint":
                String jdbcUrl = m_properties.getProperty("sqlUrl");
                if (jdbcUrl.contains("@"))
                    jdbcUrl = jdbcUrl.substring( jdbcUrl.indexOf("@")+1 );
                res.getOutputStream().print( jdbcUrl );
                break;
            default:
                res.sendError(404);
        }
    }

    public synchronized void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException
    {
        String path = req.getPathInfo();
        if (path == null)
        {
            res.sendError(404);
            return;
        }

        switch (path)
        {
            case "/start":
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(req.getInputStream()));
                if (m_exporterThread == null || m_exporterThread.getState() == Thread.State.TERMINATED)
                {
                    ZonedDateTime from = parseDateTime(reader.readLine());
                    setCurrentTimeStamp(from);
                    m_exporterThread = new ExporterThread(m_properties, from, this);
                    m_exporterThread.start();
                }
                else
                    outputText("Already running, ignoring start command");
                break;
            }
            case "/stop":
            {
                if (m_exporterThread != null)
                {
                    m_exporterThread.stopAtOpportunity.set(true);
                    try {
                        m_exporterThread.join();
                    } catch (InterruptedException e) {}
                }
                break;
            }
            default:
                res.sendError(404);
        }
    }

    public void setCurrentTimeStamp(ZonedDateTime zdt)
    {
        String formatedTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt);
        String jsonString = "{\"" + LDDB_TIMESTAMP_KEY_NAME + "\":\"" + formatedTimestamp + "\"}";

        try (Connection connection = m_postgreSQLComponent.getConnection();
             PreparedStatement statement = prepareWriteStatement(connection, jsonString))
        {
            statement.executeUpdate();
        } catch (SQLException sqlEx)
        {
            outputText("Failed to write current time to database: " + sqlEx);
        }
    }

    public synchronized void init()
    {
        m_properties = PropertyLoader.loadProperties("secret");

        m_postgreSQLComponent = new PostgreSQLComponent(m_properties.getProperty("sqlUrl"), m_properties.getProperty("sqlMaintable"));

        try (Connection connection = m_postgreSQLComponent.getConnection();
             PreparedStatement statement = prepareSelectStatement(connection);
             ResultSet resultSet = statement.executeQuery() )
        {
            while(resultSet.next())
            {
                System.out.println(statement);
                String settings = resultSet.getString("settings");

                ObjectMapper mapper = (ObjectMapper) m_postgreSQLComponent.mapper;
                Map<String, String> map = mapper.readValue(settings, Map.class);

                ZonedDateTime lastTimeStamp = ZonedDateTime.parse( map.get(LDDB_TIMESTAMP_KEY_NAME) );

                outputText("Resuming from timestamp found in database: " + lastTimeStamp);

                m_exporterThread = new ExporterThread(m_properties, lastTimeStamp, this);
                m_exporterThread.start();
            }

        } catch (SQLException | IOException ex)
        {
            outputText("Failed to read start time from database, not starting: " + ex);
        }
    }

    private PreparedStatement prepareWriteStatement(Connection connection, String json)
            throws SQLException
    {
        String tableName = m_properties.getProperty("sqlMaintable") + "__settings";
        String sql = "WITH upsertsettings AS (UPDATE " + tableName + " SET settings = ? WHERE key = ? RETURNING *)" +
                "INSERT INTO " + tableName + " (key, settings) SELECT ?,? WHERE NOT EXISTS (SELECT * FROM upsertsettings)";

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setObject(1, json, java.sql.Types.OTHER);
        statement.setString(2, LDDB_ROW_KEY_NAME);
        statement.setString(3, LDDB_ROW_KEY_NAME);
        statement.setObject(4, json, java.sql.Types.OTHER);
        return statement;
    }

    private PreparedStatement prepareSelectStatement(Connection connection)
            throws SQLException
    {
        String sql = "SELECT settings FROM " + m_properties.getProperty("sqlMaintable") + "__settings" +
                " WHERE key = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, LDDB_ROW_KEY_NAME);
        return statement;
    }

    public synchronized void destroy()
    {
        if (m_exporterThread != null)
        {
            m_exporterThread.stopAtOpportunity.set(true);
            try {
                m_exporterThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public void outputText(String text)
    {
        ZonedDateTime now = ZonedDateTime.now();
        m_pseudoConsole[m_pseudoConsoleNext++] = now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME) + ":&nbsp;&nbsp;" + text;
        if (m_pseudoConsoleNext > PSEUDO_CONSOLE_LINES-1)
            m_pseudoConsoleNext = 0;
    }

    /**
     * Construct a string representation of the pseudo console output (the last N lines of generated output)
     */
    private String getPseudoConsole()
    {
        int next = m_pseudoConsoleNext;
        StringBuilder output = new StringBuilder("");

        for (int i = 0; i < PSEUDO_CONSOLE_LINES; ++i)
        {
            --next;
            if (next < 0)
                next = PSEUDO_CONSOLE_LINES - 1;

            if (m_pseudoConsole[next] == null)
                break;

            //output.append(m_pseudoConsole[next]);
            //output.append("\n");
            output.insert(0, m_pseudoConsole[next] + "\n");
        }

        return  output.toString();
    }

    private ZonedDateTime parseDateTime(String stringTime)
    {
        if (stringTime == null || stringTime.equals("null"))
            return null;

        try
        {
            return ZonedDateTime.parse(stringTime, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        } catch (DateTimeParseException e)
        {
            return null;
        }
    }
}
