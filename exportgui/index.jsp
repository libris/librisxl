<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<html>
    <head>
        <title>export</title>
        <style type="text/css">
            .default { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 80%; }
            .rubrik { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 120%; }
        </style>
    </head>
    <body>
<%      
//        if (request.getRemoteAddr().equals("127.0.0.1") || request.getRemoteAddr().startsWith("193.10.75") || request.getRemoteAddr().startsWith("192.168.3")) {
        if (false) {        
%>
        <table cellpadding="0" cellspacing="0" width="700">
            <tr>
                <td><img src="/exportgui/images/logga.jpg"></td>
            </tr>
            <tr height="1" bgcolor="#dfdfdf">
                <td></td>
            </tr>
        </table>
        <p class="rubrik">exportadministration</p>
        <table cellpadding="0" cellspacing="0" width="700">
            <tr bgcolor="#000000">
                <td colspan="6" height="1"></td>
            </tr>
            <tr bgcolor="#dfdfdf" class="default">
                <td nowrap align="center">&nbsp;<b>exportgrupp</b>&nbsp;</td>
                <td nowrap align="center" width="100%">&nbsp;<b>namn</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>på/av</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>senaste export</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>status</b>&nbsp;</td>
                <td nowrap align="center"></td>
            </tr>
            <tr bgcolor="#000000">
                <td colspan="6" height="1"></td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="6" height="1"></td>
            </tr>
<%
            java.util.Properties prop = new java.util.Properties();
            prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
            java.io.File files[] =  new java.io.File(prop.getProperty("ProfileDir")).listFiles();

            java.util.Map<String, java.util.Properties> fileMap = new java.util.TreeMap<String, java.util.Properties>();
            for (java.io.File f: files) {
                if (f.getName().endsWith(".properties")) {
                    java.util.Properties properties = new java.util.Properties();
                    properties.load(new java.io.FileInputStream(f));
                    fileMap.put(properties.getProperty("longname"), properties);
                }
            }
            
            for (java.util.Properties properties: fileMap.values()) {
%>
            <tr>
                <td nowrap class="default">&nbsp;<a href="editprofile.jsp?name=<%out.print(properties.getProperty("name"));%>"><%out.print(properties.getProperty("name"));%></a>&nbsp;</td>
                <td nowrap class="default">&nbsp;<% out.print(properties.getProperty("longname", "")); %>&nbsp;</td>
                <td nowrap class="default" align="center">&nbsp;<a href="changestatus.jsp?name=<%out.print(properties.getProperty("name"));%>"><% out.print(properties.getProperty("mode", "")); %></a>&nbsp;</td>
                <td nowrap class="default" align="center">&nbsp;<a href="viewlogs.jsp?name=<%out.print(properties.getProperty("name"));%>"><% out.print(properties.getProperty("latest_export", "")); %></a>&nbsp;</td>
                <td nowrap class="default" align="center">&nbsp;<% out.print(properties.getProperty("status", "OK")); %>&nbsp;</td>
                <td nowrap class="default" align="center">&nbsp;<a href="deleteprofile.jsp?name=<%out.print(properties.getProperty("name"));%>">radera</a>&nbsp;</td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="6" height="1"></td>
            </tr>

<%
            }
%>
        </table>        
        <br>
        <form method="get" action="editprofile.jsp">
            <input type="text" name="name"><input type="submit" value="skapa profil">
        </form>
        <%  } else if (request.getSession().getAttribute("group") != null) {
                response.sendRedirect("showprofile.jsp?name=" + request.getSession().getAttribute("group"));
            } else { 
                java.util.Properties prop = new java.util.Properties();
                prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
                java.io.File files[] =  new java.io.File(prop.getProperty("ProfileDir")).listFiles();

                java.util.Map<String, java.util.Properties> fileMap = new java.util.TreeMap<String, java.util.Properties>();
                for (java.io.File f: files) {
                    if (f.getName().endsWith(".properties")) {
                        java.util.Properties properties = new java.util.Properties();
                        properties.load(new java.io.FileInputStream(f));
                        fileMap.put(properties.getProperty("longname"), properties);
                    }
                }
%>
            <table width="100%">
                <tr height="300">
                    <td height="100%" align="center" valign="middle">
                        <form method="GET" action="login.jsp">
                        <table>
                            <tr>
                                <td class="default">exportgrupp:</td>
                                <td>
                                    <select name="group">
<%
                                    for (java.util.Properties properties: fileMap.values()) {
%>
                                        <option value="<%out.print(properties.getProperty("name"));%>"><%out.print(properties.getProperty("longname"));%></option>
<%
                                    }
%>
                                    </select>
                                </td>
                            </tr>
                            <tr>
                                <td class="default">lösenord:</td>
                                <td><input name="password"/></td>
                            </tr>
                            <% if (request.getParameter("error") != null) {
                                if (request.getParameter("error").equals("invalidpassword")) { %>
                                <tr>
                                    <td></td>
                                    <td class="default"><font color="#ff0000">felaktigt lösenord</td>
                                </tr>
                            <% } 
                            }%>
                            <tr>
                                <td></td>
                                <td><input type="submit" value="logga in"/></td>
                            </tr>
                        </table>
                        </form>
                    </td>
                </tr>
            </table>
        <%  }  %>
    </body>
</html>
