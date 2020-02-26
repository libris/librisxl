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
        OBJECT,
        ARRAY,
        NUMBER,
        STRING,
    }

    private class Thing
    {
        JSON_TYPE jsonType;
        String property; // hasComponent/mainEntity etc

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
        String sql = "SELECT data FROM lddb TABLESAMPLE SYSTEM ( 0.001 ) limit 200;";
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
            sampleObject(doc.data, "");
    }

    private void sampleObject(Map map, String path)
    {
        for (Object key : map.keySet())
        {
            Object obj = map.get(key);
            sampleNodeData(obj, path, (String) key);
        }
    }

    private void sampleList(List list, String path)
    {
        for (Object obj : list)
        {
            sampleNodeData(obj, path, "_");
        }
    }

    private void sampleNodeData(Object obj, String path, String property)
    {
        JSON_TYPE jsonType = null;
        if (obj instanceof Map)
        {
            jsonType = JSON_TYPE.OBJECT;
            sampleObject( (Map) obj, path + property + ",");
        }
        else if (obj instanceof List)
        {
            sampleList( (List) obj, path + "_,");
        }
        else
            return;
        /*else if (obj instanceof String)
        {

        }*/

        Thing thing = new Thing();
        thing.property = property;
        thing.jsonType = jsonType;


        ThingsAtPath things = profile.get(path);
        if (things == null)
        {
            things = new ThingsAtPath();
            profile.put(path, things);
        }
        int count = things.things.get(thing);
        count++;
        things.things.put(thing, count);
        things.count++;


    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {

    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        sampleMoreRecords();
    }
}
