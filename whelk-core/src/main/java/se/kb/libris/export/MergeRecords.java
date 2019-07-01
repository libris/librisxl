package se.kb.libris.export;

import se.kb.libris.util.marc.Controlfield;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;
import se.kb.libris.util.marc.io.Iso2709Deserializer;
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import se.kb.libris.util.marc.io.StrictIso2709Reader;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MergeRecords {
    public static String format(MarcRecord mr) {
        String bibId = "", isxn = "", author = "", title = "";
        
        Iterator iter = mr.iterator("001|020|022|100|245");
        while (iter.hasNext()) {
            try {
                Field f = (Field)iter.next();

                if (f.getTag().equals("001")) {
                    bibId = ((Controlfield)f).getData();
                } else {
                    Datafield df = (Datafield)f;
                    StringBuffer sb = new StringBuffer();
                    Iterator<? extends Subfield> sfiter = df.iterator();

                    while (sfiter.hasNext()) {
                        sb.append(sfiter.next().getData());
                        sb.append(' ');
                    }

                    if (f.getTag().equals("020") || f.getTag().equals("022")) {
                        isxn = sb.toString().trim().split(" ")[0];
                        
                        if (isxn.length() > 16) isxn = isxn.substring(0, 13) + "...";
                    } else if (f.getTag().equals("100")) {
                        author = sb.toString().trim();
                        if (author.length() > 21) author = author.substring(0, 18) + "...";
                    } else if (f.getTag().equals("245")) {
                        title = sb.toString().trim();
                        if (title.length() > 30) title = title.substring(0, 30);
                    }
                }
            } catch (Exception e) {
            }
        }
        
        return String.format("%-9s %-16s %-21s %-30s", bibId, isxn, author, title);
    }
    
    public static void main(String args[]) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: java [options] se.kb.libris.export.MergeRecords <inencoding> <profile>");
            System.exit(1);
        }

        String inEncoding = args[0];
        ExportProfile profile = new ExportProfile(new File(args[1]));
        StrictIso2709Reader reader = new StrictIso2709Reader(System.in);
        byte record[] = null;
        MarcRecord bibRecord = null;
        Set<MarcRecord> auths = new HashSet<MarcRecord>();
        Map<String, MarcRecord> mfhds = new TreeMap<String, MarcRecord>();
        MarcRecord mr = null;
        MarcRecordWriter writer = null;
        
        if (profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML")) {
            writer = new MarcXmlRecordWriter(System.out, profile.getProperty("characterencoding"));
        } else {
            writer = new Iso2709MarcRecordWriter(System.out, profile.getProperty("characterencoding"));
        }

        while ((record = reader.readIso2709()) != null) {
            mr = Iso2709Deserializer.deserialize(record, inEncoding);
            
            if (mr.getType() == MarcRecord.BIBLIOGRAPHIC) {
                if (bibRecord != null) {
                    try {
                        for (MarcRecord r: profile.mergeRecord(bibRecord, mfhds, auths)) {
                            System.err.println(format(bibRecord));
                            writer.writeRecord(r);
                        }
                    } catch (Exception e) {
                        System.err.println("--- Exception while merging records ---");
                        System.err.println(e.getMessage());
                        e.printStackTrace();

                        try {
                            for (MarcRecord r: auths) System.err.println(r);
                            System.err.println(bibRecord);
                            for (MarcRecord r: mfhds.values()) System.err.println(r);
                        } catch (Exception e2) {
                            System.err.println(e2.getMessage());
                            e2.printStackTrace();
                            System.err.println("Error while printing records due to error in merge ...");
                        }
                        System.err.println("---------------------------------------");
                    }
                }
                
                bibRecord = mr;
                mfhds.clear();
                auths.clear();
            } else if (mr.getType() == MarcRecord.AUTHORITY) {
                auths.add(mr);
            } else if (mr.getType() == MarcRecord.HOLDINGS) {                
                mfhds.put(((Subfield)((Datafield)mr.iterator("852").next()).iterator("b").next()).getData(), mr);
            }
        }
        
        if (bibRecord != null) {
            for (MarcRecord r: profile.mergeRecord(bibRecord, mfhds, auths)) {
                System.err.println(format(bibRecord));
                writer.writeRecord(r);
            }                    
        } 
        
        reader.close();
        writer.close();
    }
}
