/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.api.http;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.PathParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;

/**
 * REST Web Service
 *
 * @author marma
 */
@Path("/")
public class API {

    @Context
    private UriInfo context;

    /**
     * Creates a new instance of API
     */
    public API() {
    }

    /**
     * Retrieves representation of an instance of se.kb.libris.whelks.api.http.API
     * @return an instance of java.lang.String
     */
    @GET @Produces("application/xml") @Path("{whelk}/")
    public String getXml() {
        //TODO return proper representation object
        throw new UnsupportedOperationException();
    }

    /**
     * PUT method for updating or creating an instance of API
     * @param content representation for the resource
     * @return an HTTP response with content of the updated or created resource.
     */
    @PUT
    @Consumes("application/xml")
    public void putXml(String content) {
    }
}
