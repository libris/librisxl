<%
    java.util.Properties prop = new java.util.Properties();
    prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
    java.io.File exportDir = new java.io.File(prop.getProperty("HomeDir") + "/files/" + request.getParameter("name") + "/marc");
    java.io.File f = new java.io.File(exportDir, request.getParameter("file"));
    java.io.InputStream in = new java.io.FileInputStream(f);
     
    if (request.getParameter("file").endsWith("xml")) {
        response.setContentType("text/xml");
    } else {
        response.setContentType("application/x-download");
    }
    
    response.setHeader("Content-Disposition", "attachment; filename=" + request.getParameter("file"));
    
    int i=0; 
    while ((i = in.read()) != -1) {
        out.write(i);
    }
%>