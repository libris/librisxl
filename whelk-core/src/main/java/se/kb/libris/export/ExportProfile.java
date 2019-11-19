package se.kb.libris.export;

import se.kb.libris.export.dewey.DeweyMapper;
import se.kb.libris.util.marc.Controlfield;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcFieldComparator;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExportProfile {

    public static final String BIBLEVEL_FILTER = "biblevel";
    public static final String E_FILTER = "efilter";
    public static final String FICTION_FILTER = "fictionfilter";
    public static final String LICENSE_FILTER = "licensefilter";

    public static final String FICTION_LETTERS = "1cdefhjmp";

    Properties properties = new Properties();
    Map<String, Set<String>> sets = new HashMap<String, Set<String>>();
    Map<String, String> extraFields = new HashMap<String, String>();

    public ExportProfile() {
    }

    public ExportProfile(Properties properties) {
        this.properties = properties;

        for (String extra: properties.getProperty("extrafields", "").split(";")) {
            if (extra.trim().equals("")) continue;
            String sigel = extra.trim().split(":")[0].trim();
            String fields[] = extra.trim().split(":")[1].trim().split(",");
            String f = null;

            for (int i=0;i<fields.length;i++) {
                if (f == null) {
                    f = fields[i].trim();
                } else {
                    f += "|" + fields[i].trim();
                }
            }

            if (f != null) {
                extraFields.put(sigel, f);
            }
        }
    }

    public ExportProfile(File file) throws IOException {
        properties.load(new FileInputStream(file));

        for (String extra: properties.getProperty("extrafields").split(";")) {
            if (extra.trim().equals("")) continue;
            String sigel = extra.trim().split(":")[0].trim();
            String fields[] = extra.trim().split(":")[1].trim().split(",");
            String f = null;

            for (int i=0;i<fields.length;i++) {
                if (f == null) {
                    f = fields[i].trim();
                } else {
                    f += "|" + fields[i].trim();
                }
            }

            if (f != null) {
                extraFields.put(sigel, f);
            }
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public Set<String> getSet(String name) {
        Set<String> ret = null;

        if (sets.containsKey(name)) {
            sets.get(name);
        } else {
            ret = new HashSet<String>();

            if (properties.getProperty(name) != null) {
                for (String str: properties.getProperty(name, "").split(" ")) {
                    if (str.equals("")) continue;
                    ret.add(str);
                }
            }
        }

        return ret;
    }


    public static boolean isEResource(MarcRecord mr) {
        Iterator iter = mr.iterator("007");

        if (iter.hasNext()) {
            Controlfield cf = (Controlfield)iter.next();

            return cf.getChar(0) == 'c' && cf.getChar(1) == 'r';
        }

        return false;
    }


    public static boolean isFiction(MarcRecord mr) {
        // fiction must be Text and Monograph
        if (mr.getLeader(6) != 'a' || mr.getLeader(7) != 'm') {
            return false;
        }

        Iterator iter008 = mr.iterator("008");
        if (iter008.hasNext()) {
            Controlfield cf = (Controlfield)iter008.next();
            // BooksLiteraryFormType ...
            char bookstype = cf.getChar(33);
            // is one of the fiction codes:
            return FICTION_LETTERS.indexOf(bookstype) > -1;
        }

        return false;
    }


    public static boolean isLicenseRecord(MarcRecord mr) {
        Iterator iter = mr.iterator("040");

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();
            Iterator iter2 = df.iterator("9");

            if (iter2.hasNext()) return true;
        }

        return false;
    }


    public static boolean isPrelInfo(MarcRecord mr) {
        return mr.getLeader(17) == '8';
    }

    public boolean filter(MarcRecord mr) {
        return findFilter(mr) != null;
    }

    public String findFilter(MarcRecord mr) {
        if (getProperty(BIBLEVEL_FILTER, "off").equalsIgnoreCase("ON") && isPrelInfo(mr)) {
            return BIBLEVEL_FILTER;
        }
        if (getProperty(LICENSE_FILTER, "off").equalsIgnoreCase("ON") && isLicenseRecord(mr)) {
            return LICENSE_FILTER;
        }
        if (getProperty(E_FILTER, "off").equalsIgnoreCase("ON") && isEResource(mr)) {
            return E_FILTER;
        }
        if (getProperty(FICTION_FILTER, "off").equalsIgnoreCase("ON") && isFiction(mr)) {
            return FICTION_FILTER;
        }

        return null;
    }

    /**
     * Adds pointers from 9XX fields to 100, 110, 111, 130, 240, 440,
     * 600, 610, 611, 630, 648, 650, 651, 654, 655, 700, 710, 711, 730,
     * 800, 810, 811 and 830 fields
     * @param args
     * @throws java.lang.Exception
     */

    public static MarcRecord addAuthLinks(se.kb.libris.util.marc.MarcRecord mr) {
        Pattern pattern1 = Pattern.compile(
                "100|110|111|130|240|440|600|610|" +
                "611|630|648|650|651|654|655|700|" +
                "710|711|730|800|810|811|830");
        Pattern pattern2 = Pattern.compile("9..");

        // step 1: iterate over 9XX-fields that migh contain links
        // to 1XX,6XX,7XX,8XX-fields
        Iterator iter = mr.iterator(pattern2);

        while (iter.hasNext()) {
            StringBuffer sb = new StringBuffer();
            Datafield df = (Datafield)iter.next();
            Subfield sf6 = null;
            Iterator siter = df.iterator();
            Set<String> codes = new TreeSet<String>();

            boolean uFound = false;
            while (siter.hasNext()) {
                Subfield sf = (Subfield)siter.next();
                char code = sf.getCode();

                if (code == 'u') {
                    uFound = true;
                    code = 'a';
                } else if (code == '6') {
                    sf6 = sf;
                    sf.setData("");
                }

                if (uFound) {
                    sb.append(" $" + code + " " + sf.getData());
                    codes.add(String.valueOf(code));
                }
            }

            if (sb.length() == 0) continue;

            // iterate over 1XX,6XX,7XX,8XX-fields
            Iterator iter2 = mr.iterator(pattern1);
            while (iter2.hasNext()) {
                StringBuffer sb2 = new StringBuffer();
                Datafield df2 = (Datafield)iter2.next();

                boolean hold = false, first = true;
                Iterator siter2 = df2.iterator();
                while (siter2.hasNext()) {
                    Subfield sf = (Subfield)siter2.next();

                    if (sf.getCode() == '5') {
                        break;
                    }

                    if (codes.contains(String.valueOf(sf.getCode())) || first) {
                        first = false;
                        sb2.append(" $" + sf.getCode() + " " + sf.getData());
                    }
                }

                normalize(sb);
                normalize(sb2);

                if (sb.length() > 2 && sb2.length() > 2 &&
                        sb.toString().trim().substring(2).equalsIgnoreCase(
                        sb2.toString().trim().substring(2))) {
                    //System.err.println("match: " + df2.getTag()  + " -> " + df.getTag() + " '" + sb + "'");

                    if (sf6 == null) {
                        sf6 = df.createSubfield('6', "");
                        df.listIterator().add(sf6);
                    }

                    if (sf6.getData().equals("")) {
                        sf6.setData(df2.getTag());
                    } else {
                        sf6.setData(sf6.getData() + "," + df2.getTag());
                    }
                }
            }
        }

        return mr;
    }

    /**
     * Adds ids from auth record to 1XX fields
     */

    public static MarcRecord addAuthIds(se.kb.libris.util.marc.MarcRecord mr, String p1, se.kb.libris.util.marc.MarcRecord ar, String p2) {
        Pattern pattern1 = Pattern.compile(p1);
        Pattern pattern2 = Pattern.compile(p2);

        String authId = ((Controlfield)ar.iterator("001").next()).getData();

        // iterate over fields in auth-pattern
        Iterator iter = ar.iterator(pattern2);

        while (iter.hasNext()) {
            // build string
            StringBuffer sb = new StringBuffer();
            Datafield df = (Datafield)iter.next();
            Iterator siter = df.iterator();
            Set<String> codes = new HashSet<String>();

            while (siter.hasNext()) {
                Subfield sf = (Subfield)siter.next();
                char code = sf.getCode();

                if (code == '0' || code == '6' || code == '8') continue;

                sb.append(" $" + code + " " + sf.getData());
                codes.add(String.valueOf(code));
            }

            if (sb.length() == 0) continue;

            // iterate over fields in bib-pattern and add ids on match
            Iterator iter2 = mr.iterator(pattern1);
            while (iter2.hasNext()) {
                StringBuffer sb2 = new StringBuffer();
                Datafield df2 = (Datafield)iter2.next();

                Iterator siter2 = df2.iterator();
                while (siter2.hasNext()) {
                    Subfield sf = (Subfield)siter2.next();
                    char code = sf.getCode();

                    if (codes.contains(String.valueOf(code))) {
                        sb2.append(" $" + code + " " + sf.getData());
                    }
                }

                normalize(sb);
                normalize(sb2);

                if (sb.toString().equals(sb2.toString())) {
                    df2.addSubfield('0', authId);
                }
            }
        }

        return mr;
    }

    /**
     * Remove everything except 'a'-'z','A'-'z','-' and '$'. Convert 'a'-'z' -> 'A'-'Z'
     *
     * @param sb
     */
    public static void normalize(StringBuffer sb) {
        int n=0;
        boolean whitespace = false, first = true;

        for (int i=0;i<sb.length();i++) {
            char c = sb.charAt(i);

            if (Character.isLetterOrDigit(c) || c == '-' || c == '$') {
                if (whitespace) {
                    sb.setCharAt(n++, ' ');
                }

                whitespace = false;
                sb.setCharAt(n++, Character.toUpperCase(c));
                first = false;
            } else if (Character.isWhitespace(c) && !whitespace) {
                if (!first) whitespace = true;
            }
        }

        sb.setLength(n);
    }

    public static MarcRecord move03592035a(MarcRecord mr) {
        Iterator iter = mr.iterator("035");

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();
            Iterator sfiter = df.iterator("9");

            while (sfiter.hasNext()) {
                ((Subfield)sfiter.next()).setCode('a');
            }
        }

        return mr;
    }

    public static MarcRecord addSabTitles(MarcRecord mr) {
        Iterator iter = mr.iterator("084");

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();

            Iterator sfiter = df.iterator("a|2");
            String sab = null, value = null;
            boolean ok = false;

            while (sfiter.hasNext()) {
                Subfield sf = (Subfield)sfiter.next();

                if (sf.getCode() == '2' && (sf.getData().endsWith("kssb/6") || sf.getData().endsWith("kssb/7") || sf.getData().endsWith("kssb/8") || sf.getData().endsWith("kssb/9") || sf.getData().endsWith("kssb/10"))) {
                    ok = true;
                } else if (sf.getCode() == 'a') {
                    sab = sf.getData();
                }
            }

            if (sab != null && ok) {
                boolean done = false;

                while (!done && sab.length() != 0) {
                    value = SabMadness.lookupSab(sab);

                    if (value == null) {
                        sab = sab.substring(0, sab.length()-1);
                    } else {
                        done = true;
                    }
                }
            }

            if (value != null) {
                Datafield df976 = mr.createDatafield("976");
                df976.setIndicator(0, ' ');
                df976.setIndicator(0, '0');
                df976.addSubfield('a', sab);
                df976.addSubfield('b', value);
                // @todo: sort fields using comparator
                mr.addField(df976, MarcFieldComparator.strictSorted);
            }
        }

        return mr;
    }

    public static MarcRecord hyphenateIsbn(MarcRecord mr) {
        se.kb.libris.export.IsbnHyphenator.hyphenate(mr);

        return mr;
    }

    public static MarcRecord dehyphenateIsbn(MarcRecord mr) {
        se.kb.libris.export.IsbnHyphenator.dehyphenate(mr);

        return mr;
    }

    public static MarcRecord dehyphenateIssn(MarcRecord mr) {
        Iterator iter = mr.iterator("022");

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();
            Iterator sfiter = df.iterator("a|z");

            while (sfiter.hasNext()) {
                Subfield sf = (Subfield)sfiter.next();
                String data = sf.getData();

                if (sf.getData().length() >= 9 && sf.getData().charAt(4) == '-') {
                    sf.setData(data.substring(0,4) + data.substring(5));
                }
            }
        }

        return mr;
    }

    public MarcRecord mergeBibAuth(MarcRecord bibRecord, MarcRecord authRecord) {
        Iterator iter = authRecord.iterator("1..|4..|5..|750");
        Datafield df1XX = null;
        String lang = "sw ";

        Iterator tmpiter = bibRecord.iterator("008");
        if (tmpiter.hasNext()) {
            Controlfield cf = (Controlfield)tmpiter.next();

            if (cf.getData().length() == 32) {
                lang = cf.getData().substring(15,17);
            }
        }

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();

            if (df.getTag().startsWith("1")) {
                df1XX = df;
            } else if (df.getTag().startsWith("4") || df.getTag().startsWith("5")) {
                if (df1XX == null) continue;
                Datafield df2 = bibRecord.createDatafield("9" + df.getTag().substring(1));

                //if (authid)
                //    df2.addSubfield('0', ((Controlfield)authRecord.iterator("001").next()).getData());

                // first indicator
                if (df2.getTag().equals("930")) {
                    df2.setIndicator(0, df.getIndicator(1));
                } else {
                    df2.setIndicator(0, df.getIndicator(0));
                }

                // second indicator
                if (df.getTag().charAt(0) == '4') {
                    df2.setIndicator(1, 's');
                } else if (df.getTag().charAt(0) == '5') {
                    df2.setIndicator(1, 'k');
                }

                // add subfields
                Iterator siter = df.iterator();
                while (siter.hasNext()) {
                    Subfield sf = (Subfield)siter.next();
                    // if (sf.getCode() == 'u') continue; ???????????
                    df2.addSubfield(sf.getCode(), sf.getData());
                }

                // add data from 1XX
                Iterator df1XXsiter = df1XX.iterator();
                boolean first = true;
                while (df1XXsiter.hasNext()) {
                    Subfield sf = (Subfield)df1XXsiter.next();
                    // if (sf.getCode() == 'u') continue; ???????????
                    if (sf.getCode() == '6') continue;

                    if (first) {
                        df2.addSubfield('u', sf.getData());
                        first = false;
                    } else {
                        df2.addSubfield(sf.getCode(), sf.getData());
                    }
                }

                if (df2.getSubfields().size() > 0) bibRecord.addField(df2, MarcFieldComparator.strictSorted);
            }
        }

        return bibRecord;
    }

    public MarcRecord addLcsh(MarcRecord bibRecord, MarcRecord authRecord) {
        Iterator iter = authRecord.iterator("750");
        String lang = "sw ";

        Iterator tmpiter = bibRecord.iterator("008");
        if (tmpiter.hasNext()) {
            Controlfield cf = (Controlfield)tmpiter.next();

            if (cf.getData().length() == 32) {
                lang = cf.getData().substring(15,17);
            }
        }

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();

            if (lang.equals("sw ")) {
                Datafield df2 = bibRecord.createDatafield("650");
                df2.setIndicator(0, df.getIndicator(0));
                df2.setIndicator(1, df.getIndicator(1));
                Iterator siter = df.iterator();

                while (siter.hasNext()) {
                    Subfield sf = (Subfield)siter.next();

                    df2.addSubfield(sf.getCode(), sf.getData());
                }

                bibRecord.addField(df2, MarcFieldComparator.strictSorted);
            }
        }

        return bibRecord;
    }

    public MarcRecord addExtraMfhdFields(MarcRecord bibRecord, String sigel, MarcRecord mfhdRecord) {
        String fields = extraFields.get(sigel);

        Iterator iter = mfhdRecord.iterator(fields);

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next(), df2 = bibRecord.createDatafield(df.getTag());
            df2.setIndicator(0, df.getIndicator(0));
            df2.setIndicator(1, df.getIndicator(1));
            df2.addSubfield('5', sigel);
            Iterator sfiter = df.iterator();

            while (sfiter.hasNext()) {
                Subfield sf = (Subfield)sfiter.next();
                df2.addSubfield(sf.getCode(), sf.getData());
            }

            bibRecord.addField(df2, MarcFieldComparator.strictSorted);
        }

        return bibRecord;
    }

    public MarcRecord mergeBibMfhd(MarcRecord bibRecord, String sigel, MarcRecord mfhdRecord) {
        // add 841 field
        Datafield df841 = bibRecord.createDatafield("841");
        df841.addSubfield('5', sigel);
        df841.addSubfield('a', mfhdRecord.getLeader().substring(6,10));
        df841.addSubfield('b', ((Controlfield)mfhdRecord.iterator("008").next()).getData().substring(0,32));
        df841.addSubfield('e', mfhdRecord.getLeader().substring(17,18));
        bibRecord.addField(df841);

        // add rest of fields
        Iterator iter = mfhdRecord.iterator();
        while (iter.hasNext()) {
            Field f = (Field)iter.next();

            if (f.getTag().equals("014")) {
                continue;
            } else if (f instanceof Datafield) {
                Datafield df = bibRecord.createDatafield(f.getTag());
                df.addSubfield('5', sigel);
                df.setIndicator(0, ((Datafield)f).getIndicator(0));
                df.setIndicator(1, ((Datafield)f).getIndicator(1));

                Iterator siter = ((Datafield)f).iterator();
                while (siter.hasNext()) {
                    Subfield sf = (Subfield)siter.next();
                    df.addSubfield(sf.getCode(), sf.getData());
                }

                bibRecord.addField(df);
            }
        }

        return bibRecord;
    }

    public void decompose(MarcRecord mr) {
        se.kb.libris.util.charcomposer.ComposeUtil.decompose(mr, false);
    }

    public void compose(MarcRecord mr) {
        se.kb.libris.util.charcomposer.ComposeUtil.compose(mr, false);
    }

    public void composeLatin1(MarcRecord mr) {
        Iterator<? extends Field> fiter = mr.iterator();
        StringBuffer sb = new StringBuffer();

        compose(mr);

        while (fiter.hasNext()) {
            Field f = fiter.next();

            if (f instanceof Datafield) {
                Datafield df = (Datafield)f;
                Iterator<? extends Subfield> sfiter = df.iterator();

                while (sfiter.hasNext()) {
                    Subfield sf = sfiter.next();
                    String data = sf.getData();
                    sb.setLength(0);

                    for (int i=0;i<data.length();i++) {
                        char c = data.charAt(i);

                        if (c > 0xFF) {
                            sb.append(se.kb.libris.util.charcomposer.ComposeUtil.decompose(String.valueOf(c), false));
                        } else {
                            sb.append(c);
                        }
                    }

                    sf.setData(sb.toString());
                }
            }
        }
    }

    public void move240to244(MarcRecord mr) {
        for (Datafield df: mr.getDatafields("240")) {
            Map<String, Subfield> subfieldMap = new TreeMap<String, Subfield>();

            for (Subfield sf: df.getSubfields("a|k|l|n|p"))
                subfieldMap.put(String.valueOf(sf.getCode()), sf);

            if ((subfieldMap.containsKey("l") || !mr.getFields("041").isEmpty()) && mr.getDatafields("244").isEmpty()) {
                Datafield df244 = mr.createDatafield("244");
                df244.addSubfield('s', "Orig:s titel");

                if (subfieldMap.containsKey("a") || subfieldMap.containsKey("k"))
                    df244.addSubfield(df244.createSubfield('a', ((subfieldMap.containsKey("a")? subfieldMap.get("a").getData():"") + " " + (subfieldMap.containsKey("k")? subfieldMap.get("k").getData():"")).trim()));

                if (subfieldMap.containsKey("n"))
                    df244.addSubfield('g', subfieldMap.get("n").getData());

                if (subfieldMap.containsKey("p"))
                    df244.addSubfield('g', subfieldMap.get("p").getData());

                mr.addField(df244, MarcFieldComparator.strictSorted);

                ListIterator li = mr.listIterator();
                while (li.hasNext())
                    if (((Field)li.next()).getTag().equals("240"))
                        li.remove();

                return;
            }
        }
    }

    public void move240to500(MarcRecord mr) {
        for (Datafield df: mr.getDatafields("240")) {
            Map<String, Subfield> subfieldMap = new TreeMap<String, Subfield>();

            for (Subfield sf: df.getSubfields("a|k|l|n|p"))
                subfieldMap.put(String.valueOf(sf.getCode()), sf);

            if ((subfieldMap.containsKey("l") || !mr.getFields("041").isEmpty())) {
                Datafield df500 = mr.createDatafield("500");
                df500.addSubfield('a', "Orig:s titel:" +
                        (subfieldMap.containsKey("a")? " " + subfieldMap.get("a").getData():"") +
                        (subfieldMap.containsKey("k")? " " + subfieldMap.get("k").getData():"") +
                        (subfieldMap.containsKey("n")? " " + subfieldMap.get("n").getData():"") +
                        (subfieldMap.containsKey("p")? " " + subfieldMap.get("p").getData():""));

                mr.addField(df500, MarcFieldComparator.strictSorted);

                ListIterator li = mr.listIterator();
                while (li.hasNext())
                    if (((Field)li.next()).getTag().equals("240"))
                        li.remove();

                return;
            }
        }
    }

    /*
    private boolean isSoftDeletedHoldings(MarcRecord mr) {
        for (Datafield df: mr.getDatafields("852")) {
            String preceeding = "";

            for (Subfield sf: df.getSubfields("x")) {
                if (sf.getData().equalsIgnoreCase("deleted")) {
                    // @todo remove all fields with $x with the same value of the *preceeding* $x

                    ListIterator<Field> liter = mr.listIterator();
                    while (liter.hasNext()) {
                        Field f = liter.next();

                        if (f.getTag().equals("852") || f.getTag().equals("856")) {
                            Datafield hdf = (Datafield)f;

                            for (Subfield hsf: hdf.getSubfields("x")) {
                                if (hsf.getData().equals(preceeding)) {
                                    liter.remove();
                                }
                            }
                        }
                    }
                }

                preceeding = sf.getData();
            }
        }
    }
    */

    private boolean isSoftDeletedHoldings(MarcRecord mr) {
        for (Datafield df: mr.getDatafields("852")) {
            for (Subfield sf: df.getSubfields("x")) {
                if (sf.getData().equalsIgnoreCase("deleted")) {
                    return true;
                }
            }
        }

        return false;
    }

    public Vector<MarcRecord> mergeRecord(MarcRecord bibRecord, Map<String, MarcRecord> holdings, Set<MarcRecord> auths) {
        Vector<MarcRecord> ret = new Vector<MarcRecord>();

        // filter out soft-deleted holdings records?
        if (getProperty("softdeleteholdings", "off").equalsIgnoreCase("on")) {
            Map<String, MarcRecord> tmp = new TreeMap<String, MarcRecord>(holdings);

            for (String key: holdings.keySet()) {
                if (isSoftDeletedHoldings(holdings.get(key))) {
                    tmp.remove(key);
                }
            }

            holdings = tmp;
        }

        ret.add(bibRecord);

        // add 003 to bib record
        bibRecord.addField(bibRecord.createControlfield("003", getProperty("f003", "SE-LIBR")), MarcFieldComparator.strictSorted);

        for (MarcRecord auth: auths) {
            if (!getProperty("nameform", "").equals("")) {
                changeNameForm(auth, bibRecord, getProperty("nameform"));
            }

            if (getProperty("authtype", "interleaved").equalsIgnoreCase("INTERLEAVED")) {
                bibRecord = mergeBibAuth(bibRecord, auth);
            } else if (getProperty("authtype", "interleaved").equalsIgnoreCase("AFTER")) {
                // add 003 to auth record
                auth.addField(auth.createControlfield("003", getProperty("f003", "SE-LIBR")), MarcFieldComparator.strictSorted);
                ret.add(auth);
            }

            if (getProperty("lcsh", "off").equals("on")) {
                addLcsh(bibRecord, auth);
            }

            if (getProperty("addauthids", "off").equalsIgnoreCase("ON")) {
                addAuthIds(bibRecord, "100|110|111|130|240|440|600|610|611|630|648|650|651|654|700|710|711|730|800|810|811|830", auth, "100|110|111|130");
                addAuthIds(bibRecord, "655", auth, "155");
                addAuthIds(bibRecord, "648|650|651", auth, "148|150|151");
            }
        }

        for (String sigel: holdings.keySet()) {
            MarcRecord mfhd = holdings.get(sigel);

            if (extraFields.containsKey(sigel)) {
                addExtraMfhdFields(bibRecord, sigel, mfhd);
            }
        }

        if (getProperty("move0359", "off").equalsIgnoreCase("ON")) {
            bibRecord = move03592035a(bibRecord);
        }

        if (getProperty("isbn", "dehyphenate").equalsIgnoreCase("dehyphenate")) {
            bibRecord = dehyphenateIsbn(bibRecord);
        } else if (getProperty("isbn", "dehyphenate").equalsIgnoreCase("hyphenate")) {
            bibRecord = hyphenateIsbn(bibRecord);
        }

        if (getProperty("sab", "off").equalsIgnoreCase("ON")) {
            bibRecord = addSabTitles(bibRecord);
        }

        if (getProperty("addauthlinks", "off").equalsIgnoreCase("ON")) {
            bibRecord = addAuthLinks(bibRecord);
        }

        if (getProperty("generatedewey", "off").equalsIgnoreCase("ON")) {
            DeweyMapper.addDewey(bibRecord);
        }

        if (getProperty("generatesab", "off").equalsIgnoreCase("ON")) {
            DeweyMapper.addSAB(bibRecord);
        }

        if (getProperty("move240to244", "off").equalsIgnoreCase("ON")) {
            move240to244(bibRecord);
        }

        if (getProperty("move240to500", "off").equalsIgnoreCase("ON")) {
            move240to500(bibRecord);
        }

        // Inactivate due to problems with machine-generated SAB from DEWEY
        /*if (getProperty("addxinfo", "false").equalsIgnoreCase("ON")) {
            bibRecord = addXinfo999(bibRecord);
        }*/

        for (String sigel: holdings.keySet()) {
            MarcRecord mfhd = holdings.get(sigel);

            if (getSet("locations").contains(sigel) || getSet("locations").contains("*")) {
                if (getProperty("holdtype", "interleaved").equalsIgnoreCase("INTERLEAVED")) {
                    try {
                        bibRecord = mergeBibMfhd(bibRecord, sigel, mfhd);
                    } catch (Exception e) {
                        System.err.println("--- Exception while merging bibliographic and holdings record ---");
                        System.err.println("Exception: " + e.getMessage());
                        e.printStackTrace();
                        System.err.println();
                        System.err.println(bibRecord);
                        System.err.println();
                        System.err.println(mfhd);
                        System.err.println("-----------------------------------------------------------------");
                    }
                } else if (getProperty("holdtype", "interleaved").equalsIgnoreCase("AFTER")) {
                    // add 003 to hold record
                    mfhd.addField(mfhd.createControlfield("003", getProperty("f003", "SE-LIBR")), MarcFieldComparator.strictSorted);
                    ret.add(mfhd);
                }
            }
        }

        for (MarcRecord mr: ret) {
            if (getProperty("composestrategy", "none").equals("decompose")) {
                decompose(mr);
            } else if (getProperty("composestrategy", "none").equals("compose")) {
                compose(mr);
            } else if (getProperty("composestrategy", "none").equals("composelatin1")) {
                composeLatin1(mr);
            }
        }

        return ret;
    }

    // UGLY
    private void changeNameForm(MarcRecord auth, MarcRecord bibRecord, String form) {
        Iterator fiter = auth.iterator("400");

        while (fiter.hasNext()) {
            Datafield df = (Datafield)fiter.next();
            Iterator sfiter = df.iterator("i");

            if (sfiter.hasNext()) {
                Subfield sf = (Subfield)sfiter.next();

                if (sf.getData().trim().equalsIgnoreCase(form) || sf.getData().trim().equalsIgnoreCase(form + ":")) {
                    substituteNameFields((Datafield)auth.iterator("100").next(), df, bibRecord.iterator("100|600|700"));
                    mergeNameFields(df, (Datafield)auth.iterator("100").next());
                }
            }
        }
    }

    private void substituteNameFields(Datafield from, Datafield to, Iterator iter) {
        String fromKey = generateNameKey(from), toKey = generateNameKey(to);
        //System.err.println("---");
        //System.err.println("from key: " + fromKey);
        //System.err.println("to key: " + toKey);

        while (iter.hasNext()) {
            Datafield df = (Datafield)iter.next();

            //System.err.println("comp key: " + generateNameKey(df));
            if (generateNameKey(df).equalsIgnoreCase(fromKey)) {
                mergeNameFields(to, df);
            }
        }
        //System.err.println("---");
    }

    private String generateNameKey(Datafield df) {
        StringBuffer sb = new StringBuffer();

        //build key
        Iterator iter = df.iterator();
        while (iter.hasNext()) {
            Subfield sf = (Subfield)iter.next();
            char code = sf.getCode();

            if (code != 'i' && code != 'w' && !(code >= '0' && code <= '9')) {
                sb.append("$" + Character.toUpperCase(sf.getCode()) + sf.getData());
            }
        }

        normalize(sb);

        return sb.toString();
    }

    private void mergeNameFields(Datafield to, Datafield df) {
        Iterator sfiter = to.iterator();

        while (sfiter.hasNext()) {
            Subfield sf = (Subfield)sfiter.next();
            char code = sf.getCode();

            if (code != 'i' && code != 'w' && !(code >= '0' && code <= '9')) {
                Iterator sfiter2 = df.iterator(String.valueOf(code));

                if (sfiter2.hasNext()) {
                    Subfield sf2 = (Subfield)sfiter2.next();
                    sf2.setData(sf.getData());
                }
            }
        }
    }

    private MarcRecord addXinfo999(MarcRecord bibRecord) {
        for (Entry<String, String> entry : bibRecord.getProperties().entrySet()) {
            String key = entry.getKey(), value = entry.getValue();
            // Xinfo hash key
            String xkey = key.substring(key.indexOf('_') + 1, key.lastIndexOf('_'));
            // Xinfo type
            String xtype = key.substring(key.lastIndexOf('_') + 1);
            if (xtype.equals("sum")) {
                bibRecord = create999Fields(bibRecord, value, xkey, 'b');
            }
            else if (xtype.equals("toc")) {
                bibRecord = create999Fields(bibRecord, value, xkey, 'c');
            }
            else if (xtype.equals("rev")) {
                bibRecord = create999Fields(bibRecord, value, xkey, 'd');
            }
            else if (xtype.equals("img")) {
                bibRecord = create999Fields(bibRecord, value, xkey, 'e');
            }
            else if (xtype.equals("spl")) {
                bibRecord = create999Fields(bibRecord, value, xkey, 'x');
            }
        }
        return bibRecord;
    }

    private MarcRecord create999Fields(MarcRecord bibRecord, String value, String xkey, char code) {
        // Extract strings between single quotes and put them in separate 999 fields
        //Pattern pattern = Pattern.compile("\\'([^\\'\\']*)\\'");
        Pattern pattern = Pattern.compile("\'([^']*)\'");
        Matcher matcher = pattern.matcher(value);
        while (matcher.find() && matcher.groupCount() > 0) {
            String val = matcher.group(1);
            Datafield df = bibRecord.createDatafield("999");
            df.setIndicator(0, ' ');
            df.setIndicator(1, ' ');
            df.addSubfield(df.createSubfield('a', "xinfo"));
            df.addSubfield(df.createSubfield(code, val));
            df.addSubfield(df.createSubfield('y', xkey));
            bibRecord.addField(df);
        }
        return bibRecord;
    }
}
