package trld.platform;

import java.util.*;
import java.io.*;

public class Output {

    private PrintStream out;
    private ByteArrayOutputStream bos;

    public Output() {
        bos = new ByteArrayOutputStream();
        out = new PrintStream(bos);
    }

    public Output(PrintStream out) {
        this.out = out;
    }

    public void write(String s) {
        out.print(s);
    }

    public void writeln(String s) {
        out.println(s);
    }

    public ByteArrayOutputStream getCaptured() {
        return bos;
    }

    public String getValue() {
        try {
            return bos != null ? bos.toString("utf-8") : null;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
