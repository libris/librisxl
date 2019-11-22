package whelk.rest.api

import javax.servlet.ServletOutputStream
import javax.servlet.WriteListener

class CapturingServletOutputStream extends ServletOutputStream {
    ByteArrayOutputStream out = new ByteArrayOutputStream()

    @Override
    boolean isReady() {
        return true
    }

    @Override
    void setWriteListener(WriteListener writeListener) {

    }

    @Override
    void write(int i) throws IOException {
        out.write(i)
    }

    String asString() {
        return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8)
    }
}
