<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<c:if test="${not empty param.group}">
<%
        request.getSession().setAttribute("group", request.getParameter("group"));
        response.sendRedirect("showprofile.jsp?name=" + request.getParameter("group"));
%>    
</c:if>
<c:if test="${empty param.group}">
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
%>

        <table width="100%" cellpadding="0" cellspacing="0">
            <tr bgcolor="#efefef">
                <td colspan="4"><img src="images/1trans.gif"/></td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td class="default">&nbsp;<b>kortnamn</b></td>
                <td class="default"><b>namn</b></td>
                <td class="default"><b>status</b></td>
                <td class="default"><b>last changed</b></td>
            </tr>
            <tr bgcolor="#7f7f7f">
                <td colspan="4"><img src="images/1trans.gif"/></td>
            </tr>
<%          for (java.util.Properties properties: fileMap.values()) { %>
            <tr>
                <td class="default">&nbsp;<% out.println("<a href=\"chooseprofile.jsp?group=" + properties.getProperty("name") + "\">" + properties.getProperty("name") + "</a>"); %></td>
                <td class="default"><% out.println(properties.getProperty("longname")); %></td>
                <td class="default"><% out.println(properties.getProperty("status")); %></td>
                <td class="default">
		<%
		String file_name=prop.getProperty("ProfileDir")+java.io.File.separatorChar+properties.getProperty("name")+".properties";
		long l_time = (new java.io.File(file_name)).lastModified();
		String time = ""+l_time; 
		// java.text.DateFormat df = java.text.DateFormat.getDateInstance(java.text.DateFormat.SHORT);
		java.text.DateFormat df = new java.text.SimpleDateFormat("yy.MM.dd HH:mm:ss");
		
		// out.println(file_name+":"); 
		out.println(df.format(new java.util.Date(l_time))); 
		%></td>
            </tr>
<%          } %>
        </table>
</c:if>
