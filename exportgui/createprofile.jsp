<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<c:if test="${not empty param.shortname and not empty param.longname}">
    <%
    try {
        java.util.Properties properties = new java.util.Properties();
        java.util.Properties prop = new java.util.Properties();
        prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
        java.io.File file = new java.io.File(prop.getProperty("ProfileDir") + java.io.File.separatorChar + request.getParameter("shortname") + ".properties");
        properties.setProperty("name", request.getParameter("shortname"));
        properties.setProperty("longname", request.getParameter("longname"));
        properties.store(new java.io.FileOutputStream(file), null);
        request.getSession().setAttribute("group", request.getParameter("shortname"));
        response.sendRedirect("showprofile.jsp?operation=editprofile&name=" + request.getParameter("shortname"));
    } catch (Exception e) {%>
<html>
<body>
    <% 
        out.println(e.getMessage());
        out.println("<br>");
        java.io.PrintWriter pw = new java.io.PrintWriter(out);
        e.printStackTrace(pw);
    %>
</body>
</html>
    
    <%
    }
    %>
</c:if>
<html>
<body>
<table width="100%">
    <tr height="300">
        <td height="100%" align="center" valign="middle">
            <form method="GET" action="createprofile.jsp">
            <table>
                <tr>
                    <td class="default">profilnamn (endast a-z,0-9):</td>
                    <td><input name="shortname"/></td>
                </tr>
                <tr>
                    <td class="default">långt namn:</td>
                    <td><input name="longname"/></td>
                </tr>
                <tr>
                    <td></td>
                    <td><input type="submit" value="skapa profil"/></td>
                </tr>
            </table>
            </form>
        </td>
    </tr>
</table>
</body>
</html>

