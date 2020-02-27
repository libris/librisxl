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
        ADD,
        VALIDATE,
    }

    private class Thing
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
            if (o instanceof Thing)
            {
                Thing t = (Thing) o;
                return jsonType.equals(t.jsonType) && property.equals(t.property);
            }
            return false;
        }
    }

    private class ThingsAtPath
    {
        public ThingsAtPath()
        {
            things = new HashMap<>();
        }
        HashMap<Thing, Integer> things; // The variants of things seen at this path mapped to the number of such observations
        long count; // The _total_ count of things observed at this path
    }

    private HashMap<String, ThingsAtPath> profile = new HashMap<>();

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
                OPERATION op = OPERATION.ADD;
                sampleNodeData(doc.data, "root,", op);
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void sampleNodeData(Object obj, String path, OPERATION op)
    {
        if (obj instanceof Map)
        {
            Map map = (Map) obj;
            for (Object key : ((Map)obj).keySet())
            {
                Thing thing = new Thing();
                thing.jsonType = JSON_TYPE.OBJECT;
                thing.property = (String) key;
                sampleNodeData( map.get(key), path + key + lookForwardForType(map.get(key)) + ",", op);
                if (op == OPERATION.ADD)
                    addToProfile(path, thing);
                else
                    validateObject(path, thing, obj);
            }

        }
        else if (obj instanceof List)
        {
            for (Object next : (List) obj)
            {
                Thing thing = new Thing();
                thing.jsonType = JSON_TYPE.ARRAY;
                sampleNodeData( next, path + "[]" + lookForwardForType(next) + ",", op);
                if (op == OPERATION.ADD)
                    addToProfile(path, thing);
                else
                    validateObject(path, thing, obj);
            }
        }
        else if (obj instanceof String)
        {
            Thing thing = new Thing();
            thing.jsonType = JSON_TYPE.STRING;
            if (op == OPERATION.ADD)
                addToProfile(path, thing);
            else
                validateObject(path, thing, obj);
        }
        else if (obj instanceof Integer || obj instanceof Long || obj instanceof Float || obj instanceof Double)
        {
            Thing thing = new Thing();
            thing.jsonType = JSON_TYPE.NUMBER;
            if (op == OPERATION.ADD)
                addToProfile(path, thing);
            else
                validateObject(path, thing, obj);
        }
        else if (obj instanceof Boolean)
        {
            Thing thing = new Thing();
            thing.jsonType = JSON_TYPE.BOOLEAN;
            if (op == OPERATION.ADD)
                addToProfile(path, thing);
            else
                validateObject(path, thing, obj);
        }
        else if (obj == null)
        {
            Thing thing = new Thing();
            thing.jsonType = JSON_TYPE.NULL;
            if (op == OPERATION.ADD)
                addToProfile(path, thing);
            else
                validateObject(path, thing, obj);
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

    private void addToProfile(String path, Thing thing)
    {
        ThingsAtPath things = profile.get(path);
        if (things == null)
        {
            things = new ThingsAtPath();
            profile.put(path, things);
        }
        Integer count = things.things.get(thing);
        if (count != null)
            things.things.put(thing, count + 1);
        else
            things.things.put(thing, 1);
        things.count++;
    }

    private void validateObject(String path, Thing thing, Object data)
    {
        ThingsAtPath things = profile.get(path);
        if (things == null)
            return;
        Integer count = things.things.get(thing);
        if (count == null)
        {
            System.out.println("Did not expect " + thing.property + " (of type " + thing.jsonType + ") at " + path + " suggestions:");
            for (Thing t : things.things.keySet())
                System.out.println("\t" + t.property + " / " + t.jsonType + " " + (100.0f * (float)things.things.get(t) / (float)things.count) + "%");
        }
    }

    private void printProfile()
    {
        for (Object key : profile.keySet())
        {
            ThingsAtPath thingsAtPath = profile.get(key);
            System.out.println(key + " total: " + thingsAtPath.count);
            for (Thing t : thingsAtPath.things.keySet())
                System.out.println("\t" + t.property + " / " + t.jsonType + " count: " + thingsAtPath.things.get(t));

        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        lock.readLock().lock();
        try
        {
            String content = IOUtils.toString(request.getReader());
            Map body = PostgreSQLComponent.mapper.readValue(content, HashMap.class);

            boolean addNotAnnotate = false;
            sampleNodeData(body, "root,", OPERATION.VALIDATE);

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/ld+json");
            response.setCharacterEncoding("UTF-8");
            PrintWriter out = response.getWriter();
            out.print(PostgreSQLComponent.mapper.writeValueAsString(body));
            out.close();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {

    }
}
