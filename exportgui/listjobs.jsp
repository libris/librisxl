<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@include file="logincheck.jsp"%>

<%
        java.util.Properties prop = new java.util.Properties();
        prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
        java.io.File jobDir = new java.io.File(prop.getProperty("HomeDir") + "/jobs/incoming");
        java.io.File files[] = new java.io.File[0];
        
        if (jobDir.exists()) {
            files = jobDir.listFiles();
        }
%>
        <table width="100%" cellpadding="0" cellspacing="0">
            <tr bgcolor="#efefef">
                <td colspan="3"><img src="images/1trans.gif"/></td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td class="default">&nbsp;<b>exportdatum</b></td>
                <td class="default"><b>period</b></td>
                <td class="default"></b></td>
            </tr>
            <tr bgcolor="#7f7f7f">
                <td colspan="3"><img src="images/1trans.gif"/></td>
            </tr>
<%
        java.util.Set<java.util.Properties> set = new java.util.TreeSet<java.util.Properties>(new java.util.Comparator() { public int compare(Object o1, Object o2) { 
            java.util.Properties p1 = (java.util.Properties) o1, p2 = (java.util.Properties)o2; 
                return (p1.getProperty("year") + p1.getProperty("month") + p1.getProperty("day_in_month")).compareTo((p2.getProperty("year") + p2.getProperty("month") + p2.getProperty("day_in_month")));
            }});
            
        for (java.io.File f: files) {
            java.util.Properties p = new java.util.Properties();
            java.io.InputStream os = new java.io.FileInputStream(f);
            p.load(os);
            os.close();
            
            if (p.getProperty("name").equals(request.getParameter("name"))) {
                set.add(p);
                p.setProperty("filename", f.getName());
            }
        }

        for (java.util.Properties p: set) { %>
            <tr>
                <td class="default">&nbsp;<% if (p.getProperty("exporttype", "").equals("fulldump")) { out.println("kommande helg"); } else { out.print(p.getProperty("year") + "-" + p.getProperty("month") + "-" + p.getProperty("day_in_month")); }%></td>
                <td class="default"><% if (p.getProperty("exporttype", "").equals("fulldump")) { out.println("fullständigt uttag"); } else { out.print(p.getProperty("start") + " -> " + p.getProperty("stop")); }%></td>
                <td class="default"><a href="deletejob.jsp?name=${param.name}&jobid=<% out.print(p.getProperty("filename")); %>">ta bort</td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="4"><img src="images/1trans.gif"/></td>
            </tr>
<%      } %>
        </table>
