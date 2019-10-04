<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
    <%
        java.util.Properties properties = new java.util.Properties();
        java.util.Properties prop = new java.util.Properties();
        prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
        java.io.File file = new java.io.File(prop.getProperty("ProfileDir") + java.io.File.separatorChar + request.getParameter("name") + ".properties");
        properties.load(new java.io.FileInputStream(file));        
        session.setAttribute("properties", properties);
    %>
    
        <form action="saveprofile.jsp">
            <input type="hidden" name="name" value="<% out.print(properties.getProperty("name", "")); %>">
            <table width="100%">
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;allmänt</b></td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;status</td>
                    <td>
                        <select name="status">
                            <option value="on" <% if (properties.getProperty("status", "off").equals("on")) out.print("selected"); %>>PÅ</option>
                            <option value="off" <% if (properties.getProperty("status", "off").equals("off")) out.print("selected"); %>>AV</option>
                        </select>
                    </td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;långt namn:</td>
                    <td><input type="text" name="longname" value="<% out.print(properties.getProperty("longname", "")); %>" size="50"/></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;urval - beståndsposter</b></td>
                </tr>
                <tr>
                    <td colspan="2" class="default">
                        &nbsp;&nbsp;exportera bibliografiska poster när tillhörande beståndspost: <input type="checkbox" name="holdcreate" <% if (properties.getProperty("holdcreate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>skapas&nbsp;<input type="checkbox" name="holdupdate" <% if (properties.getProperty("holdupdate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/> ändras&nbsp;<input type="checkbox" name="holddelete" <% if (properties.getProperty("holddelete", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/> tas&nbsp;bort
                    </td>
                </tr>
                <tr>
                    <td nowrap class="default">&nbsp;&nbsp;begränsa operatörer:</td>
                    <td><input type="text" size="30" name="holdoperators" value="<% out.print(properties.getProperty("holdoperators", "")); %>"/></td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;begränsa sigler:</td>
                    <td><input type="text" size="50" name="locations" value="<% out.print(properties.getProperty("locations", "")); %>"/></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;urval - bibliografisk post</b></td>
                </tr>
                <tr>
                    <td colspan="2" class="default">
                        &nbsp;&nbsp;exportera bibliografiska poster när de: <input type="checkbox" name="bibcreate" <% if (properties.getProperty("bibcreate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>skapas&nbsp;<input type="checkbox" name="bibupdate" <% if (properties.getProperty("bibupdate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/> ändras<br/>
                    </td>
                </tr>
                <tr>
                    <td nowrap class="default">&nbsp;&nbsp;begränsa operatörer:</td>
                    <td><input type="text" size="30" name="biboperators" value="<% out.print(properties.getProperty("biboperators", "")); %>"/></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;urval - auktoritetsposter</b></td>
                </tr>
                <tr>
                    <td colspan="2" class="default">
                        &nbsp;&nbsp;exportera bibliografiska poster när länkad auktoritetspost: <input type="checkbox" name="authcreate" <% if (properties.getProperty("authcreate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>skapas&nbsp;<input type="checkbox" name="authupdate" <% if (properties.getProperty("authupdate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/> ändras<br/>
                    </td>
                </tr>
                <tr>
                    <td nowrap class="default">&nbsp;&nbsp;begränsa operatörer:</td>
                    <td><input type="text" size="30" name="authoperators" value="<% out.print(properties.getProperty("authoperators", "")); %>"/></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;bearbetning</b> (<a target="_new" href="https://www.kb.se/libris/Libris-metadatflode/Anvandning/Batchexport/instruktioner/#bearbetning">förklaring</a>)</td>
                </tr>
                <tr>
                    <td class="default" colspan="2">
                        &nbsp;&nbsp;<input type="checkbox" name="sab" <% if (properties.getProperty("sab", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>SAB-rubriker
                        &nbsp;&nbsp;<input type="checkbox" name="generatesab" <% if (properties.getProperty("generatesab", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>Generera SAB från Dewey
                        &nbsp;&nbsp;<input type="checkbox" name="generatedewey" <% if (properties.getProperty("generatedewey", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>Generera Dewey från SAB
                        &nbsp;&nbsp;<input type="checkbox" name="lcsh" <% if (properties.getProperty("lcsh", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>LC subject headings
                        &nbsp;&nbsp;<input type="checkbox" name="isbndehyphenate" <% if (properties.getProperty("isbndehyphenate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>ta bort streck i ISBN
                        &nbsp;&nbsp;<input type="checkbox" name="isbnhyphenate" <% if (properties.getProperty("isbnhyphenate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>lägg till streck i ISBN<br>
                        &nbsp;&nbsp;<input type="checkbox" name="issndehyphenate" <% if (properties.getProperty("issndehyphenate", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>ta bort streck i ISSN
                        &nbsp;&nbsp;<input type="checkbox" name="move0359" <% if (properties.getProperty("move0359", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>035$9 -> 035$a
                        &nbsp;&nbsp;<input type="checkbox" name="move240to244" <% if (properties.getProperty("move240to244", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>240 -> 244
                        <!--&nbsp;&nbsp;<input type="checkbox" name="move240to500" <% if (properties.getProperty("move240to250", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>240 -> 500-->

                    </td>
                </tr>
                <tr>
                    <td colspan="2" class="default"></td>
                </tr>
                <tr>
                    <td valign="top" class="default">&nbsp;&nbsp;extra beståndsfält:</td>
                <td><input type="text" size="50" name="extrafields" value="<% out.print(properties.getProperty("extrafields", "")); %>"/></td>
                </tr>
                <tr>
                    <td></td>
                    <td valign="top" class="default"><font size="-1">&nbsp;&nbsp;&lt;sigel&gt;:&lt;fält[,fält,fält,fält,...]&gt; ; &lt;sigel&gt;:... o.s.v.<br>&nbsp;&nbsp;<i>exempel: T:082,650 ; X:650</i></font></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;filtrera bort</b></td>
                </tr>
                <tr>
                    <td class="default" colspan="2">
                        &nbsp;&nbsp;<input type="checkbox" name="efilter" <% if (properties.getProperty("efilter", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>e-resurser
                        &nbsp;&nbsp;<input type="checkbox" name="biblevel" <% if (properties.getProperty("biblevel", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>prel. info
                        &nbsp;&nbsp;<input type="checkbox" name="licensefilter" <% if (properties.getProperty("licensefilter", "").equalsIgnoreCase("ON")) out.print("checked=\"checked\""); %>/>licens-poster
                    </td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;Namnformer</b></td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;namnform (person)</td>
                    <td><select name="nameform"><option value="" <% if (properties.getProperty("name", "").equalsIgnoreCase("")) out.print("selected"); %>>standard</option><option value="Forskningsbiblioteksform" <% if (properties.getProperty("nameform", "").equalsIgnoreCase("Forskningsbiblioteksform")) out.print("selected"); %>>forskningsbiblioteksform</option></select></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;format</b></td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;003-sträng:</td>
                    <td width="100%"><select name="f003"><option value="SE-LIBR" <% if (properties.getProperty("f003", "SE-LIBR").equalsIgnoreCase("SE-LIBR")) out.print("selected"); %>>SE-LIBR</option><option value="LIBRIS" <% if (properties.getProperty("f003", "SE-LIBR").equalsIgnoreCase("LIBRIS")) out.print("selected"); %>>LIBRIS</option></select></td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;postkodning:</td>
                    <td width="100%"><select name="format"><option value="ISO2709" <% if (properties.getProperty("format", "").equalsIgnoreCase("ISO2709")) out.print("selected"); %>>ISO2709</option><option value="MARCXML" <% if (properties.getProperty("format", "").equalsIgnoreCase("MARCXML")) out.print("selected"); %>>MARCXML</option></select></td>
                </tr>
                <tr class="default">
                    <td>&nbsp;&nbsp;teckenuppsättning:</td>
                    <td><select name="characterencoding"><option value="UTF-8" <% if (properties.getProperty("characterencoding", "").equalsIgnoreCase("UTF-8")) out.print("selected"); %>>UTF-8</option><option value="Latin1Strip" <% if (properties.getProperty("characterencoding", "").equalsIgnoreCase("ISO-8859-1") || properties.getProperty("characterencoding", "").equalsIgnoreCase("Latin1Strip")) out.print("selected"); %>>ISO-8859-1</option></select></td>
                </tr>
                <tr class="default">
                    <td>&nbsp;&nbsp;diakrithantering:</td>
                    <td><select name="composestrategy"><option value="composelatin1" <% if (properties.getProperty("composestrategy", "").equalsIgnoreCase("composelatin1")) out.print("selected"); %>>prekomponera latin-1 subset</option><option value="compose" <% if (properties.getProperty("composestrategy", "composelatin1").equalsIgnoreCase("compose")) out.print("selected"); %>>prekomponera alla</option><option value="decompose" <% if (properties.getProperty("composestrategy", "composelatin1").equalsIgnoreCase("decompose")) out.print("selected"); %>>dekomponera alla</option></select> <a target="_new" href="https://www.kb.se/libris/Libris-metadatflode/Anvandning/Batchexport/instruktioner/#diakrithantering">förklaring</a></td>
                </tr>
                <tr class="default">
                    <td>&nbsp;&nbsp;auktoritetsposter:</td>
                    <td><select name="authtype"><option value="interleaved" <% if (properties.getProperty("authtype", "").equalsIgnoreCase("interleaved")) out.print("selected"); %>>sammanslagna med bib.-post</option><option value="after" <% if (properties.getProperty("authtype", "").equalsIgnoreCase("after")) out.print("selected"); %>>efter posten</option><option value="none" <% if (properties.getProperty("authtype", "").equalsIgnoreCase("none")) out.print("selected"); %>>släng</option></select></td>
                </tr>
                <tr class="default">
                    <td>&nbsp;&nbsp;beståndsposter:</td>
                    <td><select name="holdtype"><option value="interleaved" <% if (properties.getProperty("holdtype", "").equalsIgnoreCase("interleaved")) out.print("selected"); %>>sammanslagna med bib.-post</option><option value="after" <% if (properties.getProperty("holdtype", "").equalsIgnoreCase("after")) out.print("selected"); %>>efter posten</option><option value="none" <% if (properties.getProperty("holdtype", "").equalsIgnoreCase("none")) out.print("selected"); %>>släng</option></select></td>
                </tr>
<%      
    /* if (request.getRemoteAddr().equals("127.0.0.1") || request.getRemoteAddr().startsWith("193.10.75") || request.getRemoteAddr().startsWith("192.168.3") || request.getRemoteAddr().equals("193.10.249.131") || request.getRemoteAddr().startsWith("10.50.64") || request.getRemoteAddr().startsWith("10.50.67") || request.getRemoteAddr().startsWith("10.50.71") || request.getRemoteAddr().startsWith("10.50.79") || request.getRemoteAddr().startsWith("10.50.66")) { */
	if ( request.getSession().getAttribute("admin").equals("admin") ) {
%>
                <tr>
                    <td class="default">&nbsp;&nbsp;efterbehandling:</td>
                    <td width="100%">
                        <select name="post">
                            <option value="" <% if (properties.getProperty("post", "").equalsIgnoreCase("")) out.print("selected"); %>></option>
                            <option value="bokrondellen" <% if (properties.getProperty("post", "").equalsIgnoreCase("bokrondellen")) out.print("selected"); %>>Bokrondellen</option>
                            <option value="google" <% if (properties.getProperty("post", "").equalsIgnoreCase("google")) out.print("selected"); %>>Google Scholar</option>
                        </select>
                    </td>
                </tr>    
<%
    }
%>

                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;tidpunkt för export</b> (<a target="_new" href="https://www.kb.se/libris/Libris-metadatflode/Anvandning/Batchexport/instruktioner/#periodicitet">exempel</a>)</td>
                </tr>
                <tr>
                    <td class="default" colspan="2">
                        &nbsp;&nbsp;år:&nbsp;
                        <select name="year">
                            <option value="*" <% if (properties.getProperty("year", "*").equalsIgnoreCase("*")) out.print("selected"); %>>*</option>
                            <option value="2005" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2005")) out.print("selected"); %>>2005</option>
                            <option value="2006" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2006")) out.print("selected"); %>>2006</option>
                            <option value="2007" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2007")) out.print("selected"); %>>2007</option>
                            <option value="2008" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2008")) out.print("selected"); %>>2008</option>
                            <option value="2009" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2009")) out.print("selected"); %>>2009</option>
                            <option value="2010" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2010")) out.print("selected"); %>>2010</option>
                            <option value="2011" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2011")) out.print("selected"); %>>2011</option>
                            <option value="2012" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2012")) out.print("selected"); %>>2012</option>
                            <option value="2013" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2013")) out.print("selected"); %>>2013</option>
                            <option value="2014" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2014")) out.print("selected"); %>>2014</option>
                            <option value="2015" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2015")) out.print("selected"); %>>2015</option>
                            <option value="2016" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2016")) out.print("selected"); %>>2016</option>
                            <option value="2017" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2017")) out.print("selected"); %>>2017</option>
                            <option value="2018" <% if (properties.getProperty("year", "*").equalsIgnoreCase("2018")) out.print("selected"); %>>2018</option>
                        </select>
                        &nbsp;&nbsp;månad:&nbsp;
                        <select name="month">
                            <option value="*" <% if (properties.getProperty("month", "*").equalsIgnoreCase("*")) out.print("selected"); %>>*</option>
                            <option value="1" <% if (properties.getProperty("month", "*").equalsIgnoreCase("1")) out.print("selected"); %>>januari</option>
                            <option value="2" <% if (properties.getProperty("month", "*").equalsIgnoreCase("2")) out.print("selected"); %>>february</option>
                            <option value="3" <% if (properties.getProperty("month", "*").equalsIgnoreCase("3")) out.print("selected"); %>>mars</option>
                            <option value="4" <% if (properties.getProperty("month", "*").equalsIgnoreCase("4")) out.print("selected"); %>>april</option>
                            <option value="5" <% if (properties.getProperty("month", "*").equalsIgnoreCase("5")) out.print("selected"); %>>maj</option>
                            <option value="6" <% if (properties.getProperty("month", "*").equalsIgnoreCase("6")) out.print("selected"); %>>juni</option>
                            <option value="7" <% if (properties.getProperty("month", "*").equalsIgnoreCase("7")) out.print("selected"); %>>juli</option>
                            <option value="8" <% if (properties.getProperty("month", "*").equalsIgnoreCase("8")) out.print("selected"); %>>augusti</option>
                            <option value="9" <% if (properties.getProperty("month", "*").equalsIgnoreCase("9")) out.print("selected"); %>>september</option>
                            <option value="10" <% if (properties.getProperty("month", "*").equalsIgnoreCase("10")) out.print("selected"); %>>oktober</option>
                            <option value="11" <% if (properties.getProperty("month", "*").equalsIgnoreCase("11")) out.print("selected"); %>>november</option>
                            <option value="12" <% if (properties.getProperty("month", "*").equalsIgnoreCase("12")) out.print("selected"); %>>december</option>
                        </select>
                        &nbsp;&nbsp;dag i månad:
                        <select name="day_in_month">
                            <option value="*" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("*")) out.print("selected"); %>>*</option>
                            <option value="1" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("1")) out.print("selected"); %>>1</option>
                            <option value="2" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("2")) out.print("selected"); %>>2</option>
                            <option value="3" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("3")) out.print("selected"); %>>3</option>
                            <option value="4" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("4")) out.print("selected"); %>>4</option>
                            <option value="5" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("5")) out.print("selected"); %>>5</option>
                            <option value="6" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("6")) out.print("selected"); %>>6</option>
                            <option value="7" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("7")) out.print("selected"); %>>7</option>
                            <option value="8" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("8")) out.print("selected"); %>>8</option>
                            <option value="9" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("9")) out.print("selected"); %>>9</option>
                            <option value="10" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("10")) out.print("selected"); %>>10</option>
                            <option value="11" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("11")) out.print("selected"); %>>11</option>
                            <option value="12" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("12")) out.print("selected"); %>>12</option>
                            <option value="13" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("13")) out.print("selected"); %>>13</option>
                            <option value="14" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("14")) out.print("selected"); %>>14</option>
                            <option value="15" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("15")) out.print("selected"); %>>15</option>
                            <option value="16" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("16")) out.print("selected"); %>>16</option>
                            <option value="17" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("17")) out.print("selected"); %>>17</option>
                            <option value="18" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("18")) out.print("selected"); %>>18</option>
                            <option value="19" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("19")) out.print("selected"); %>>19</option>
                            <option value="20" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("20")) out.print("selected"); %>>20</option>
                            <option value="21" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("21")) out.print("selected"); %>>21</option>
                            <option value="22" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("22")) out.print("selected"); %>>22</option>
                            <option value="23" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("23")) out.print("selected"); %>>23</option>
                            <option value="24" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("24")) out.print("selected"); %>>24</option>
                            <option value="25" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("25")) out.print("selected"); %>>25</option>
                            <option value="26" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("26")) out.print("selected"); %>>26</option>
                            <option value="27" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("27")) out.print("selected"); %>>27</option>
                            <option value="28" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("28")) out.print("selected"); %>>28</option>
                            <option value="29" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("29")) out.print("selected"); %>>29</option>
                            <option value="30" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("30")) out.print("selected"); %>>30</option>
                            <option value="31" <% if (properties.getProperty("day_in_month", "*").equalsIgnoreCase("31")) out.print("selected"); %>>31</option>
                        </select>
                        &nbsp;&nbsp;dag i vecka:
                        <select name="day_in_week">
                            <option value="*" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("*")) out.print("selected"); %>>*</option>
                            <option value="Mon" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Mon")) out.print("selected"); %>>måndag</option>
                            <option value="Tue" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Tue")) out.print("selected"); %>>tisdag</option>
                            <option value="Wed" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Wed")) out.print("selected"); %>>onsdag</option>
                            <option value="Thu" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Thu")) out.print("selected"); %>>torsdag</option>
                            <option value="Fri" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Fri")) out.print("selected"); %>>fredag</option>
                            <option value="Sat" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Sat")) out.print("selected"); %>>lördag</option>
                            <option value="Sun" <% if (properties.getProperty("day_in_week", "*").equalsIgnoreCase("Sun")) out.print("selected"); %>>söndag</option>
                        </select>
                    </td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;exportperiod</b></td>
                </tr>
                <tr>
                    <td class="default" colspan="2">
                        &nbsp;&nbsp;längd:&nbsp;
                        <select name="period">
                            <option value="1" <% if (properties.getProperty("period", "1").equalsIgnoreCase("1")) out.print("selected"); %>>en dag</option>
                            <option value="7" <% if (properties.getProperty("period", "1").equalsIgnoreCase("7")) out.print("selected"); %>>en vecka</option>
                            <option value="10" <% if (properties.getProperty("period", "1").equalsIgnoreCase("10")) out.print("selected"); %>>10 dagar</option>
                            <option value="month" <% if (properties.getProperty("period", "1").equalsIgnoreCase("month")) out.print("selected"); %>>en månad</option>
                            <option value="year" <% if (properties.getProperty("period", "1").equalsIgnoreCase("year")) out.print("selected"); %>>ett år</option>
                        </select>
                    </td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                
<!--                <tr>
                    <td class="default">&nbsp;&nbsp;period</td>
                    <td class="default">
                        <select name="periodicity">
                            <option value="daily" <% if (properties.getProperty("periodicity", "").equalsIgnoreCase("daily")) out.print("selected"); %>>1 dag</option>
                            <option value="weekly" <% if (properties.getProperty("periodicity", "").equalsIgnoreCase("weekly")) out.print("selected"); %>>1 vecka</option>
                            <option value="tendays" <% if (properties.getProperty("periodicity", "").equalsIgnoreCase("tendays")) out.print("selected"); %>>10 dagar</option>
                            <option value="monthly" <% if (properties.getProperty("periodicity", "").equalsIgnoreCase("monthly")) out.print("selected"); %>>1 månad</option>
                        </select>
                    </td>
                </tr>
                <tr>
                    <td class="default">&nbsp;&nbsp;start</td>
                    <td class="default">
                        <select name="start">
                            <option value="daily" <% if (properties.getProperty("start", "").equalsIgnoreCase("daily")) out.print("selected"); %>>varje dag</option>
                            <option value="mondays" <% if (properties.getProperty("start", "").equalsIgnoreCase("mondays")) out.print("selected"); %>>måndagar</option>
                            <option value="tuesdays" <% if (properties.getProperty("start", "").equalsIgnoreCase("tuesdays")) out.print("selected"); %>>tisdagar</option>
                            <option value="wednesdays" <% if (properties.getProperty("start", "").equalsIgnoreCase("wednesdays")) out.print("selected"); %>>onsdagar</option>
                            <option value="thursdays" <% if (properties.getProperty("start", "").equalsIgnoreCase("thursdays")) out.print("selected"); %>>torsdagar</option>
                            <option value="fridays" <% if (properties.getProperty("start", "").equalsIgnoreCase("fridays")) out.print("selected"); %>>fredagar</option>
                            <option value="saturdays" <% if (properties.getProperty("start", "").equalsIgnoreCase("saturdays")) out.print("selected"); %>>lördagar</option>
                            <option value="sundays" <% if (properties.getProperty("start", "").equalsIgnoreCase("sundays")) out.print("selected"); %>>söndagar</option>
                            <option value="first" <% if (properties.getProperty("start", "").equalsIgnoreCase("first")) out.print("selected"); %>>1:a varje månad</option>
                        </select>
                    </td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>-->
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;leverans</b></td>
                </tr>
                <tr class="default">
                    <td colspan="2">
                        &nbsp;&nbsp;<input type="radio" name="delivery_type" value="LIBRISFTP" <% if (properties.getProperty("delivery_type", "LIBRISFTP").equalsIgnoreCase("LIBRISFTP")) out.print("checked=\"checked\""); %>>LIBRIS FTP-server (i mappen '/pub/export2/<% out.print(request.getParameter("name")); %>/marc')
                        &nbsp;&nbsp;<input type="radio" name="delivery_type" value="EXTFTP" <% if (properties.getProperty("delivery_type", "").equalsIgnoreCase("EXTFTP")) out.print("checked=\"checked\""); %>>egen FTP-server (fyll i nedan)
                    </td>
                </tr>
                <tr class="default">
                    <td>&nbsp;&nbsp;server:</td><td><input name="ftpserver" value="<% out.print(properties.getProperty("ftpserver", "")); %>"></td>
                </tr>
                <tr class="default">
                    <td>&nbsp;&nbsp;användare:</td><td><input name="ftpuser" value="<% out.print(properties.getProperty("ftpuser", "")); %>"></td>
                </tr>
                <tr class="default">    
                    <td>&nbsp;&nbsp;lösenord:</td><td><input name="ftppassword" value="<% out.print(properties.getProperty("ftppassword", "")); %>"></td>
                </tr>
                <tr class="default">    
                    <td>&nbsp;&nbsp;katalog:</td><td><input name="ftpdirectory" value="<% out.print(properties.getProperty("ftpdirectory", "")); %>"></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr bgcolor="#dfdfdf">
                    <td colspan="2" class="default"><b>&nbsp;e-post</b></td>
                </tr>
                <tr>
                    <td nowrap class="default">&nbsp;&nbsp;statusrapport till:</td>
                    <td><input type="text" size="30" name="contact" value="<% out.print(properties.getProperty("contact", "")); %>"/></td>
                </tr>
                <tr>
                    <td nowrap class="default">&nbsp;&nbsp;felrapport till:</td>
                    <td><input type="text" size="30" name="errcontact" value="<% out.print(properties.getProperty("errcontact", "")); %>"/></td>
                </tr>
                <tr><td colspan="2" class="default">&nbsp;</td></tr>
                <tr>    
                    <td colspan="2" align="center"><input type="submit" value="spara">&nbsp;<input type="reset" value="återställ"></form></tr>
                </tr>
            </table>
        </form>
