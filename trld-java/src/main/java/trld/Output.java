package trld;

import java.util.*;
import java.io.*;

public class Output {

    private PrintStream out;
    private ByteArrayOutputStream bos;

    public Output() {
        this(false);
    }

    public Output(boolean capture) {
        if (capture) {
            bos = new ByteArrayOutputStream();
            out = new PrintStream(bos);
        } else {
            out = System.out;
        }
    }

    public void write(String s) {
        out.print(s);
    }

    public void writeln(String s) {
        out.println(s);
    }

    public String getValue() {
        try {
            return bos != null ? bos.toString("utf-8") : null;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
