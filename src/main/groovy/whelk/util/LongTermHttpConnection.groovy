package whelk.util

import java.nio.charset.Charset
import javax.net.ssl.SSLSocketFactory

/**
 * Based on the apix_exporter/longtermhttprequest. Using both socket SO_KEEPALIVE and http Connection:keep-alive
 * to keep connections open and alive for as long as possible.
 */
public class LongTermHttpConnection
{
    private int m_responseCode
    private String m_responseData
    private HashMap<String, String> m_responseHeaders
    private Socket m_socket
    private int m_port
    private URL m_properUrl

    public LongTermHttpConnection(String host)
    {
        m_properUrl = new URL(host)
        m_port = m_properUrl.getPort()
        if (m_port == -1)
            m_port = m_properUrl.getDefaultPort()
        if (m_port == -1)
            m_port = 80
    }

    /**
     * contentType, data, basicAuthName and basicAuthPass may all be passed as null where they are not relevant.
     */
    public void sendRequest(String path, String verb, String contentType, String data,
                            String basicAuthName, String basicAuthPass)
            throws IOException
    {
        int attempts = 0
        while (true)
        {
            try
            {
                if (m_socket == null || m_socket.isClosed() || !m_socket.isConnected() || m_socket.isInputShutdown() || m_socket.isOutputShutdown()) {
                    m_socket = createSocket(m_properUrl.getProtocol(), m_properUrl.getHost(), m_port)
                    m_socket.setKeepAlive(true)
                }

                writeRequest(m_socket.getOutputStream(), m_properUrl.getHost(), path, verb, contentType,
                        data, basicAuthName, basicAuthPass)
                readResponse(m_socket.getInputStream())
                break // We're done, no need for retries
            } catch (SocketException se)
            {
                if (attempts > 2)
                    throw se

                // Close socket and retry with a new connection
                try { m_socket.close() } catch (Throwable e) { /* ignore */ }
                m_socket = null
            }
            ++attempts
        }
    }

    public void close() throws IOException
    {
        m_socket.close()
    }

    public int getResponseCode()
    {
        return m_responseCode
    }

    public String getResponseData()
    {
        return m_responseData
    }

    public Map<String, String> getResponseHeaders()
    {
        return Collections.unmodifiableMap(m_responseHeaders)
    }

    private Socket createSocket(String protocol, String host, int port)
            throws IOException
    {
        if (protocol.equals("https"))
        {
            SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault()
            return ssf.createSocket(host, port)
        }
        else
            return new Socket(host, port)
    }

    private void writeRequest(OutputStream outputStream, String host, String path, String verb, String contentType,
                              String data, String basicAuthName, String basicAuthPass)
            throws IOException
    {
        if (path.equals(""))
            path = "/"

        StringBuilder header = new StringBuilder()
        header.append( verb + " " + path + " HTTP/1.1\r\n" )
        header.append( "Host: " + host + "\r\n" )
        header.append( "Accept-Charset: utf-8\r\n" )
        header.append( "Connection: keep-alive\r\n" )

        if (basicAuthName != null && basicAuthPass != null)
        {
            Base64.Encoder encoder = Base64.getMimeEncoder()
            String basicString = basicAuthName + ":" + basicAuthPass
            String basicBase64 = new String(encoder.encode( basicString.getBytes("UTF-8")), Charset.forName("UTF-8"))
            header.append( "Authorization: Basic " + basicBase64 + "\r\n")
        }

        byte[] dataBytes = null
        if (data != null)
        {
            dataBytes = data.getBytes(Charset.forName("UTF-8"))
            header.append( "Content-Type: " + contentType + "\r\n" )
            header.append( "Content-Length: " + dataBytes.length + "\r\n" )
        }
        header.append( "\r\n" )

        outputStream.write(header.toString().getBytes(Charset.forName("UTF-8")))

        if (dataBytes != null)
            outputStream.write(dataBytes)

        outputStream.flush()
    }

    private void readResponse(InputStream inputStream)
            throws IOException
    {
        ByteArrayOutputStream completeResponse = new ByteArrayOutputStream()
        byte[] buf = new byte[1024*8]

        int bytesRead = 0
        int totalBytesRead = 0
        int contentLength = Integer.MAX_VALUE
        while (bytesRead != -1 && totalBytesRead < contentLength)
        {
            bytesRead = inputStream.read(buf)
            completeResponse.write(buf, 0, bytesRead)
            totalBytesRead += bytesRead

            //System.out.print( new String(buf, "UTF-8") ) // print all raw http to terminal

            if (contentLength == Integer.MAX_VALUE) // If we haven't parsed the content-length yet
            {
                String responseText = completeResponse.toString("UTF-8").toLowerCase()
                int contentLengthHeaderBeginsAt
                if ( (contentLengthHeaderBeginsAt = responseText.indexOf("content-length:")) != -1)
                {
                    int lineEndAt = responseText.indexOf("\r\n", contentLengthHeaderBeginsAt)
                    if (lineEndAt > contentLengthHeaderBeginsAt)
                    {
                        String contentLengthString = responseText.substring(contentLengthHeaderBeginsAt + "content-length:".length(), lineEndAt)
                        contentLength = Integer.parseInt(contentLengthString.trim())
                    }
                }
            }
        }

        // Response completely retrieved.
        String responseText = completeResponse.toString("UTF-8")
        int introEnd = responseText.indexOf("\r\n")
        if (introEnd == -1)
            throw new IOException("Malformed HTTP response, no intro line ending: " + responseText)
        String introLine = responseText.substring(0, introEnd)

        // The intro line should look something like: HTTP/1.1 200 OK
        if (!introLine.startsWith("HTTP/1."))
            throw new IOException("Malformed HTTP response, no 'HTTP/1.X': " + responseText)
        // Next three chars should be the response code.
        m_responseCode = Integer.parseInt( introLine.substring(9, 12) )

        int headerEnd = responseText.indexOf("\r\n\r\n")
        if (headerEnd == -1)
            throw new IOException("Malformed HTTP response, no correct header ending: " + responseText)

        String headerString = responseText.substring(introEnd, headerEnd).trim()
        m_responseData = responseText.substring(headerEnd+3)

        String[] headerLines = headerString.split("\r\n")
        m_responseHeaders = new HashMap<String, String>()
        for (String headerLine : headerLines)
        {
            int delimiterIndex = headerLine.indexOf(':')
            if (delimiterIndex == -1)
                throw new IOException("Malformed HTTP response, bad header line: " + headerLine)
            m_responseHeaders.put(
                    headerLine.substring(0, delimiterIndex).trim(),
                    headerLine.substring(delimiterIndex+1, headerLine.length()).trim())
        }
    }
}

