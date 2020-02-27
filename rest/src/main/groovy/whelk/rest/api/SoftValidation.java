package whelk.rest.api;

import org.apache.commons.io.IOUtils;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SoftValidation extends HttpServlet
{
    private Whelk whelk = null;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Actually storing 100 million libris IDs in an array/list would require at least 1,7Gb of memory (too much).
    // A set (which is what we would need here) would be _much_ worse still.
    // This way, a true/false lookup-version of the same information (lossy compression through potential
    // hash collisions) takes up only about 12,5 Mb.
    private final int ID_SET_SIZE = 100 * 1000 * 1000;
    private BitSet alreadyIndexed = new BitSet( ID_SET_SIZE );

    private enum JSON_TYPE
    {
        UNDEFINED,
        OBJECT,
        ARRAY,
        NUMBER,
        STRING,
        BOOLEAN,
        NULL,
    }

    private enum OPERATION
    {
        ADD_TO_PROFILE,
        VALIDATE,
    }

    private class Observation
    {
        JSON_TYPE jsonType = JSON_TYPE.UNDEFINED;
        String property = ""; // hasComponent/mainEntity etc

        @Override
        public int hashCode()
        {
            return jsonType.hashCode() + property.hashCode();
        }

        @Override
        public boolean equals(Object o)
        {
            if (o instanceof Observation)
            {
                Observation t = (Observation) o;
                return jsonType.equals(t.jsonType) && property.equals(t.property);
            }
            return false;
        }
    }

    private class ObservationsAtPath
    {
        public ObservationsAtPath()
        {
            observations = new HashMap<>();
        }
        HashMap<Observation, Integer> observations; // The variants of things seen at this path mapped to the number of such observations
        long count; // The _total_ count of things observed at this path
    }

    private HashMap<String, ObservationsAtPath> profile = new HashMap<>();

    public void init()
    {
        whelk = Whelk.createLoadedCoreWhelk();

        new Timer("SoftValidation", true).schedule(new TimerTask()
        {
            public void run()
            {
                try
                {
                    sampleMoreRecords();
                } catch (Throwable e)
                {
                    // LOG AND IGNORE
                }
            }
        }, 10*1000, 10*1000);
    }

    private void sampleMoreRecords()
    {
        lock.writeLock().lock();
        try
        {

            //String sql = "SELECT data FROM lddb TABLESAMPLE SYSTEM ( 0.001 ) WHERE collection in ('hold', 'bib') limit 200;";
            String sql = "SELECT data FROM lddb TABLESAMPLE SYSTEM ( 0.1 ) WHERE collection in ('hold', 'bib') limit 200;";
            ArrayList<Document> documents = new ArrayList<>(200);
            try(Connection connection = whelk.getStorage().getConnection();
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rs = statement.executeQuery())
            {
                while (rs.next())
                {
                    Document doc = new Document(PostgreSQLComponent.mapper.readValue(rs.getString("data"), HashMap.class));

                    String id = doc.getShortId();
                    int hash = id.hashCode();
                    hash = hash % ID_SET_SIZE;
                    if (hash < 0)
                        hash = -hash;

                    if ( alreadyIndexed.get( hash ) )
                        continue;
                    alreadyIndexed.set( hash );
                    documents.add(doc);
                }

            } catch (SQLException | IOException e)
            {
                // LOG AND IGNORE
            }

            for (Document doc : documents)
            {
                OPERATION op = OPERATION.ADD_TO_PROFILE;
                traverseData(doc.data, "root,", op, null, new ArrayList());
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void traverseData(Object obj, String profileKey, OPERATION op, Map result, List path)
    {
        if (obj instanceof Map)
        {
            Map map = (Map) obj;
            for (Object key : ((Map)obj).keySet())
            {
                Observation observation = new Observation();
                observation.jsonType = JSON_TYPE.OBJECT;
                observation.property = (String) key;
                ArrayList newPath = new ArrayList(path);
                newPath.add(key);
                traverseData( map.get(key), profileKey + key + lookForwardForType(map.get(key)) + ",", op, result, newPath);
                addToProfileOrValidate(profileKey, observation, op, result, newPath, obj);
            }

        }
        else if (obj instanceof List)
        {
            for (int i = 0; i < ((List)obj).size(); ++i)
            {
                Object next = ((List)obj).get(i);
                Observation observation = new Observation();
                observation.jsonType = JSON_TYPE.ARRAY;
                ArrayList newPath = new ArrayList(path);
                newPath.add(i);
                traverseData( next, profileKey + "[]" + lookForwardForType(next) + ",", op, result, newPath);
                addToProfileOrValidate(profileKey, observation, op, result, newPath, obj);
            }
        }
        else if (obj instanceof String)
        {
            Observation observation = new Observation();
            observation.jsonType = JSON_TYPE.STRING;
            addToProfileOrValidate(profileKey, observation, op, result, path, obj);
        }
        else if (obj instanceof Integer || obj instanceof Long || obj instanceof Float || obj instanceof Double)
        {
            Observation observation = new Observation();
            observation.jsonType = JSON_TYPE.NUMBER;
            addToProfileOrValidate(profileKey, observation, op, result, path, obj);
        }
        else if (obj instanceof Boolean)
        {
            Observation observation = new Observation();
            observation.jsonType = JSON_TYPE.BOOLEAN;
            addToProfileOrValidate(profileKey, observation, op, result, path, obj);
        }
        else if (obj == null)
        {
            Observation observation = new Observation();
            observation.jsonType = JSON_TYPE.NULL;
            addToProfileOrValidate(profileKey, observation, op, result, path, obj);
        }
    }

    private String lookForwardForType(Object obj)
    {
        if (obj instanceof Map)
        {
            Map map = (Map) obj;
            String type = (String) map.get("@type");
            if (type == null)
                return "";
            else
                return ":" + type;
            }
        return "";
    }

    private void addToProfileOrValidate(String profileKey, Observation observation, OPERATION op, Map result, List path, Object obj)
    {
        if (op == OPERATION.ADD_TO_PROFILE)
        {
            ObservationsAtPath observationsAtPath = profile.get(profileKey);
            if (observationsAtPath == null)
            {
                observationsAtPath = new ObservationsAtPath();
                profile.put(profileKey, observationsAtPath);
            }
            Integer count = observationsAtPath.observations.get(observation);
            if (count != null)
                observationsAtPath.observations.put(observation, count + 1);
            else
                observationsAtPath.observations.put(observation, 1);
            observationsAtPath.count++;
        }
        else if (op == OPERATION.VALIDATE)
        {
            ObservationsAtPath observationsAtPath = profile.get(profileKey);
            if (observationsAtPath == null)
                return;
            Integer count = observationsAtPath.observations.get(observation);
            if (count == null)
            {
                if (!result.keySet().contains("warnings"))
                    result.put("warnings", new ArrayList());
                List warnings = (List) result.get("warnings");

                HashMap warning = new HashMap();
                warnings.add(warning);
                warning.put("at", path);

                String explanation = observation.property.equals("") ?
                        "Did not expect JSON " + observation.jsonType + " here.":
                        "Did not expect \"" + observation.property + "\" JSON " + observation.jsonType + " here.";
                warning.put("explanation", explanation);

                List<String> suggestions = new ArrayList<>();
                for (Observation t : observationsAtPath.observations.keySet())
                {
                    if ( obj instanceof Map && !((Map)obj).keySet().contains(t.property) )
                        suggestions.add(t.property);
                }
                if (suggestions.size() > 0)
                    warning.put("suggestions", suggestions);

            }
        }
    }

    private void printProfile()
    {
        for (Object key : profile.keySet())
        {
            ObservationsAtPath observationsAtPath = profile.get(key);
            System.out.println(key + " total: " + observationsAtPath.count);
            for (Observation t : observationsAtPath.observations.keySet())
                System.out.println("\t" + t.property + " / " + t.jsonType + " count: " + observationsAtPath.observations.get(t));

        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

        lock.readLock().lock();
        try
        {
            String content = IOUtils.toString(request.getReader());
            Map body = PostgreSQLComponent.mapper.readValue(content, HashMap.class);

            Map result = new HashMap();
            traverseData(body, "root,", OPERATION.VALIDATE, result, new ArrayList());

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            PrintWriter out = response.getWriter();
            out.print(PostgreSQLComponent.mapper.writeValueAsString(result));
            out.close();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {

    }
}
