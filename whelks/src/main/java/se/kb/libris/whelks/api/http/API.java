package se.kb.libris.whelks.api.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import javax.annotation.security.RolesAllowed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.StreamingOutput;
import se.kb.libris.whelks.Whelk;

/**
 * REST API
 *
 * @author marma
 */
@Path("/")
public class API {
    @Context private UriInfo context;

    public API() {
    }

    @GET @Produces("application/xml") @Path("{whelk}/_raw/store") /* @RolesAllowed("api_raw") */
    public StreamingOutput rawStore(@PathParam("whelk") final String whelk, @Context final HttpServletRequest req) {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException, WebApplicationException {
                Whelk w = null; // WhelkManager(whelk);
                
                if (w instanceof RawHttpAccessible) {
                    URL url = new URL("");
                    InputStream is = null;

                    try {
                        is = url.openConnection().getInputStream();

                        byte buf[] = new byte[1024];
                        int n=-1;
                        while ((n = is.read(buf)) != -1)
                            out.write(buf, 0, n);
                    } finally {
                        try { is.close(); } catch (Exception e2) {}
                    }
                } else {
                    throw new WebApplicationException(Status.NOT_FOUND);
                }
            }
        };
    }
}
