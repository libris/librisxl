<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<html>
<%
    java.io.File files[] =  new java.io.File(application.getRealPath("/logs/" + request.getParameter("name"))).listFiles();
%>
    <head>
        <title>visa loggar</title>
        <style type="text/css">
            .default { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 80%; }
            .rubrik { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 120%; }
        </style>
    </head>
    <body>
        <table cellpadding="0" cellspacing="0" width="700">
            <tr>
                <td><img src="/export2/images/logga.jpg"></td>
            </tr>
            <tr height="1" bgcolor="#dfdfdf">
                <td></td>
            </tr>
        </table>
        <p class="rubrik">exportadministration (<% out.print(request.getParameter("name")); %>)</p>    
        <table cellpadding="0" cellspacing="0" width="700">
            <tr bgcolor="#000000">
                <td colspan="6" height="1"></td>
            </tr>
            <tr bgcolor="#dfdfdf" class="default">
                <td nowrap align="center">&nbsp;<b>exportgrupp</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>start</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>stop</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>antal poster</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>status</b>&nbsp;</td>
            </tr>
            <tr bgcolor="#000000">
                <td colspan="6" height="1"></td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="6" height="1"></td>
            </tr>
<%
            java.util.Map<Long, java.io.File> fileMap = new java.util.TreeMap<Long, java.io.File>();
            for (java.io.File f: files) {
                if (f.getName().endsWith(".log")) {
                    fileMap.put(new Long(f.lastModified()), f);
                }
            }
            
            for (java.io.File f: fileMap.values()) {
                java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(f));
                String s[] = reader.readLine().split("\t");
                reader.close();
%>
            <tr class="default">
                <td nowrap align="center">&nbsp;<% out.print(s[0]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[1]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[2]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<a href="viewlog.jsp?name=<%out.print(s[0]);%>&logname=<%out.print(f.getName());%>"><% out.print(s[3]); %></a>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[4]); %>&nbsp;</td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="6" height="1"></td>
            </tr>
<%
            }
%>

            <tr>
                <td>
        </table>
  </body>
</html>
