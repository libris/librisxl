package se.kb.libris.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

public class SabMadness {
    static Map<String, String> map = null;

    static String lookupSab(String sab) {
        if (map == null) {
            map = new TreeMap<String, String>();

            BufferedReader reader = null;

            try {
                reader = new BufferedReader(new InputStreamReader(SabMadness.class.getResourceAsStream("/se/kb/libris/export/sabrub.txt"), "ISO-8859-1"));
            } catch (Exception e) {
              System.err.println(e.getMessage());
              e.printStackTrace();
            }

            String line = null;

            try {
                while ((line = reader.readLine()) != null) {
                    int idx = line.indexOf('\t');

                    if (idx == -1) {
                        //System.err.println("no tab in line ('" + line + "')");
                        continue;
                    } else {
                        String from = line.substring(0, idx);
                        String to = line.substring(idx+1);

                        //System.err.println(from + " -> " + to);
                        map.put(from, to);
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }

        return map.get(sab);
    }

    public static void main(String args[]) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;

        while ((line = reader.readLine()) != null) {
            System.out.println(lookupSab(line));
        }
    }
}
