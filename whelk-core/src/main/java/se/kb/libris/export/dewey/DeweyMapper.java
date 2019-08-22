/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package se.kb.libris.export.dewey;

import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.MarcFieldComparator;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;
import se.kb.libris.util.marc.impl.MarcRecordImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author marma
 */
public class DeweyMapper {
    static Pattern p = Pattern.compile("^([0-9]+)\t([^\t]+)\t([^\t]+)\t([^\t]*)$");
    static Pattern sabp = Pattern.compile("kssb/[6-9]");
    static Map<String, List<Mapping>> deweyMap = null;
    static Map<String, List<Mapping>> sabMap = null;
    static long lastInit = -1;
    static long MAX_CACHE_TIME = 60*60*1000;

    private static synchronized boolean check() {
        if (deweyMap == null || sabMap == null || System.currentTimeMillis() - lastInit > MAX_CACHE_TIME)
            return init();
        else
            return true;
    }

    private static synchronized boolean init() {
        boolean ret = initFromUrl() || initFromFile() || initFromClasspath();
        
        if (ret) lastInit = System.currentTimeMillis();

        return ret;
    }

    private static synchronized boolean initFromUrl() {
        try {
            URL url = new URL(getURL());
            File cacheFile = new File(getCacheFile());

            if (!cacheFile.exists() || (System.currentTimeMillis() - cacheFile.lastModified() > MAX_CACHE_TIME)) {
                File tmpFile = File.createTempFile("deweysab_tmpfile", null);

                InputStreamReader sr = new InputStreamReader(url.openStream(), "ISO8859-1");
                String data = new Scanner(sr).useDelimiter("\\Z").next();
                Writer w = new FileWriter(tmpFile);
                w.write(data);
                w.close();
                sr.close();

                //tmpFile.renameTo(cacheFile);
                moveFile(tmpFile, cacheFile);

            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        return initFromFile();
    }

    private static synchronized boolean initFromFile() {
        File file = new File(getCacheFile());

        try {
            return file.exists() && initFromReader(new FileReader(file));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static synchronized boolean initFromClasspath() {
        try {
            return initFromReader(new InputStreamReader(DeweyMapper.class.getResourceAsStream(getClasspath())));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static synchronized boolean initFromReader(Reader r) {
        BufferedReader br = null;
        Map<String, List<Mapping>> _deweyMap = new TreeMap<String, List<Mapping>>();
        Map<String, List<Mapping>> _sabMap = new TreeMap<String, List<Mapping>>();

        try {
            br = new BufferedReader(r);


            String line = null;
            while ((line = br.readLine()) != null) {
                //System.err.println(line);
                Matcher m = p.matcher(line);
                    
                if (m.find()) {
                    int rowid = Integer.parseInt(m.group(1));
                    String sab = m.group(2), dewey = m.group(3), flags[] = m.group(4).split("\\s");
                    Relation relation = Relation.getRelation(flags);
                    Usage usage = Usage.getUsage(flags);

                    Mapping mapping = new Mapping(rowid, sab, dewey, relation, usage);

                    if (!_deweyMap.containsKey(dewey)) _deweyMap.put(dewey, new LinkedList<Mapping>());
                    _deweyMap.get(dewey).add(mapping);

                    if (!_sabMap.containsKey(sab)) _sabMap.put(sab, new LinkedList<Mapping>());
                    _sabMap.get(sab).add(mapping);

                    //System.err.println(mapping);
                }
            }

            if (deweyMap != null) deweyMap.clear();
            if (sabMap != null) sabMap.clear();

            deweyMap = _deweyMap;
            sabMap = _sabMap;

            return true;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            try { br.close(); } catch (Throwable t) {}
            try { r.close(); } catch (Throwable t) {}
        }
    }

    private static String getURL() {
        return System.getProperty("librisexport.deweysab_url", "http://export.libris.kb.se/DS/dewey_text.asp");
    }

    private static String getCacheFile() {
        return System.getProperty("librisexport.deweysab_cachefile", "/tmp/dewey-sab.cache");
    }

    private static String getClasspath() {
        return System.getProperty("librisexport.deweysab_resource", "/se/kb/libris/export/dewey/dewey_sab.txt");
    }

    private static String findDewey(List<Mapping> l) {
        Mapping mapping = null;
        int score = -1;

        for (Mapping m: l) {
            if (m.getDeweyScore() > score) {
                score = m.getDeweyScore();
                mapping = m;
            }
        }

        return (mapping != null)? mapping.dewey:null;
    }

    private static String findSAB(List<Mapping> l) {
        Mapping mapping = null;
        int score = -1;

        for (Mapping m: l) {
            if (m.getDeweyScore() > score) {
                score = m.getSabScore();
                mapping = m;
            }
        }

        return (mapping != null)? mapping.sab:null;
    }

    public static String getDewey(String sab) {
        if (sab == null || sab.length() == 0) return null;

        check();

        List<Mapping> l = sabMap.get(sab);

        if (l == null) {
            if (sab.contains(" ")) return getDewey(sab.substring(0, sab.indexOf(' ')));
            else return getDewey(sab.substring(0, sab.length()-1));
        } else if (l.size() == 1) {
            return l.get(0).dewey;
        } else {
            return findDewey(l);
        }
    }

    public static String getSAB(String dewey) {
        if (dewey == null || dewey.length() == 0) return null;

        check();

        List<Mapping> l = deweyMap.get(dewey);

        if (l == null) {
            if (dewey.contains(" ")) return getSAB(dewey.substring(0, dewey.indexOf(' ')));
            else return getSAB(dewey.substring(0, dewey.length()-1));
        } else if (l.size() == 1) {
            return l.get(0).sab;
        } else {
            return findSAB(l);
        }
    }

    public static boolean addDewey(MarcRecord mr) {
        List<String> deweys = getDeweyCodes(mr), sabs = getSABCodes(mr);
        Set<String> deweySet = new HashSet<String>();

        if (check() && deweys.isEmpty() && !sabs.isEmpty()) {
            for (String sab: sabs) {
                String dewey = getDewey(sab);

                if (dewey != null) {
                    if (!deweySet.contains(dewey))
                        mr.addField(mr.createDatafield((deweySet.isEmpty())? "082":"083").setIndicator(0, '0').setIndicator(1, deweySet.isEmpty()? '4':' ').addSubfield('a', dewey).addSubfield('2', "22 (machine generated)"), MarcFieldComparator.strictSorted);

                    deweySet.add(dewey);

                    if (deweySet.size() == 3) return true;
                }
            }
        }

        return !deweySet.isEmpty();
    }

    public static boolean addSAB(MarcRecord mr) {
        List<String> deweys = getDeweyCodes(mr), sabs = getSABCodes(mr);
        Set<String> sabSet = new HashSet<String>();

        if (check() && !deweys.isEmpty() && sabs.isEmpty()) {

            for (String dewey: deweys) {
                /** @todo remove this temporary fix when 78* and 8* classes are done */
                if (dewey.startsWith("78") || dewey.startsWith("8")) continue;
                
                String sab = getSAB(dewey);

                if (sab != null) {
                    if (!sabSet.contains(sab))
                        mr.addField(mr.createDatafield("084").addSubfield('a', sab).addSubfield('2', "kssb/8 (machine generated)"), MarcFieldComparator.strictSorted);

                    sabSet.add(sab);

                    if (sabSet.size() == 3) return true;
                }
            }
        }

        return !sabSet.isEmpty();
    }

    private static List<String> getSABCodes(MarcRecord mr) {
        List<String> ret = new LinkedList<String>();
        Iterator i = mr.iterator("084");

        while (i.hasNext()) {
            Iterator i2 = ((Datafield)i.next()).iterator("a|2");

            List<String> data = new LinkedList<String>();
            boolean ok = false;
            while (i2.hasNext()) {
                Subfield sf = (Subfield)i2.next();

                if (sf.getCode() == 'a') {
                    data.add(sf.getData());
                } else if (sf.getCode() == '2') {
                    if (sf.getData().startsWith("kssb")) {
                        ret.addAll(data);
                        data = new LinkedList<String>();
                        ok = true;
                    } else {
                        data = new LinkedList<String>();
                        ok = false;
                    }
                }
            }

            if (ok) ret.addAll(data);
        }

        return ret;
    }

    private static List<String> getDeweyCodes(MarcRecord mr) {
        List<String> ret = new LinkedList<String>();
        Iterator i = mr.iterator("082");

        while (i.hasNext()) {
            Iterator i2 = ((Datafield)i.next()).iterator("a");

            while (i2.hasNext()) {
                ret.add(((Subfield)i2.next()).getData().replace("/", "").replace("'", ""));
            }
        }

        return ret;
    }

    public static void moveFile(File sourceFile, File destFile) throws IOException {
        if (sourceFile.renameTo(destFile)) return;

        // ok, silly time
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;

        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            if (source != null) {
                source.close();
            }

            if (destination != null) {
                destination.close();
            }

            sourceFile.delete();
        }
    }

    public static void main(String args[]) {
        long t0 = System.currentTimeMillis();

        MarcRecord mr = new MarcRecordImpl();
        mr.addField(mr.createDatafield("084").addSubfield('a', "Qadd-a:bf Europeiska gemenskap").addSubfield('2', "kssb/8"));

        DeweyMapper.addDewey(mr);
        DeweyMapper.addSAB(mr);
        System.out.println(mr);

        /*
        int n=0;
        for (String sab: DeweyMapper.sabMap.keySet()) {
            if (sabMap.get(sab).size() > 1) {
                boolean hasMain = false;

                for (Mapping m: sabMap.get(sab)) {
                    if (m.getRelation() == Relation.EXACT || m.getUsage() == Usage.MAIN || m.getUsage() == Usage.RIGHT)
                        hasMain = true;
                }

                if (!hasMain) {
                    System.out.println(sabMap.get(sab));
                   n++;
                }
            }
        }

        System.out.println(n);
        System.out.println(System.currentTimeMillis() - t0);
        */
    }
}
