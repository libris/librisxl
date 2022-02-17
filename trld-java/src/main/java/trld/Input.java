package trld;

import java.util.*;
import java.io.*;

public class Input implements Closeable {

    private BufferedReader reader;
    private boolean useStdin = false;

    public Input() {
    }

    public Input(String path) {
        try {
            path = Common.removeFileProtocol(path);
            reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(path), "utf-8"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Input(boolean useStdin) {
        this.useStdin = useStdin;
        if (!useStdin) {
            return;
        }
        try {
            reader = new BufferedReader(new InputStreamReader(System.in, "utf-8"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String read() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> liter = iterlines();
        while (liter.hasNext()) {
            sb.append(liter.next()).append("\n");
        }
        return sb.toString();
    }

    public Iterable<String> lines() {
        return () -> iterlines();
    }

    Iterator<String> iterlines() {
        return new Iterator<String>() {
            String line = null;
            public boolean hasNext() {
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return line != null;
            }
            public String next() {
                return line;
            }
        };
    }

    public Iterable<String> characters() {
        return () -> iterchars();
    }

    Iterator<String> iterchars() {
        return new Iterator<String>() {
            Character c = null;
            public boolean hasNext() {
                try {
                    int i = reader.read();
                    c = i != -1 ? Character.valueOf((char) i) : null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return c != null;
            }
            public String next() {
                return c.toString();
            }
        };
    }

    public void close() {
        if (useStdin) {
            return;
        }
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
