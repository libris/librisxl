package whelk.rest.api;

import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SoftValidation extends HttpServlet
{
    private Whelk whelk = null;

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
    }

    private void sampleMoreRecords()
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
            sampleNodeData(doc.data, "root,");
    }

    private void sampleNodeData(Object obj, String path)
    {
        if (obj instanceof Map)
        {
            Map map = (Map) obj;
            for (Object key : ((Map)obj).keySet())
            {
                Thing thing = new Thing();
                thing.jsonType = JSON_TYPE.OBJECT;
                thing.property = (String) key;
                sampleNodeData( map.get(key), path + key + lookForwardForType(map.get(key)) + ",");
                addToProfile(path, thing);
            }

        }
        else if (obj instanceof List)
        {
            for (Object next : (List) obj)
            {
                Thing thing = new Thing();
                thing.jsonType = JSON_TYPE.ARRAY;
                sampleNodeData( next, path + "[]" + lookForwardForType(next) + ",");
                addToProfile(path, thing);
            }
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
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        sampleMoreRecords();
        printProfile();
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {

    }
}
