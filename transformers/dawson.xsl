<?xml version="1.0" encoding="UTF-8"?>
<!-- Last version: 3/4 2009: Notification when sigel is missing -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version="1.0"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xmlns:java="http://xml.apache.org/xslt/java"
                exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
    <xsl:template match="/marc:collection">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="marc:record"/>
        </collection>
    </xsl:template>

    <xsl:template match="marc:record">
        <xsl:choose>
            <xsl:when test="normalize-space(marc:datafield[@tag='948']/marc:subfield[@code='l'])">
                <!-- FUNK Villkor för e-böcker -->
                <xsl:variable name="has007cr">
                    <xsl:if test="count(marc:controlfield[@tag = '007' and normalize-space(substring(., 1, 2)) = 'cr']) > 0">true</xsl:if>
                </xsl:variable>

                <!-- FUNK Villkor för leader -->
                <!--<xsl:variable name="has040a">
                    <xsl:if test="marc:datafield[@tag = '040' and marc:subfield[@code = 'a']]">true</xsl:if>
                </xsl:variable>-->


                <xsl:variable name="isPreliminary">
                    <xsl:if test="count(marc:datafield[@tag = '040' and marc:subfield[@code = 'a']]) = 0 or marc:datafield[@tag = '947' and marc:subfield[@code = 'a' and (contains(normalize-space(.), 'Updated') or contains(normalize-space(.), 'Upgraded'))]]">true</xsl:if>
                </xsl:variable>

                <!-- FUNK Villkor för 040 och 856 <xsl:if test="count(marc:datafield[@tag = '856' and contains(marc:subfield[@code = 'u'], 'http://www.dawsonera.com/')]) > 0">true</xsl:if>-->
                <xsl:variable name="df856u_has_dawsonera">
                    <xsl:if test="marc:datafield[@tag = '856' and contains(marc:subfield[@code = 'u'], 'http://www.dawsonera.com/')]">true</xsl:if>
                </xsl:variable>

                <!-- Timestamp -->
                <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
                <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>

                <record type="Bibliographic">
                    <!-- FUNK Leader -->
                    <xsl:apply-templates select="marc:leader"><xsl:with-param name="isPreliminary" select="$isPreliminary"/></xsl:apply-templates>

                    <!-- FUNK Controlfield -->
                    <xsl:apply-templates select="marc:controlfield[@tag != '001']"><xsl:with-param name="has007cr" select="$has007cr"/><xsl:with-param name="timeStamp" select="$timeStamp"/></xsl:apply-templates>

                    <xsl:if test="count(marc:controlfield[@tag = '008']) = 0">
                        <xsl:choose>
                            <xsl:when test="$has007cr = 'true'">
                                <controlfield tag="008"><xsl:value-of select="$timeStamp"/>nuuuuuuuuxx ||||  |||||||| ||und d</controlfield>
                            </xsl:when>
                        <xsl:otherwise>
                                <controlfield tag="008"><xsl:value-of select="$timeStamp"/>nuuuuuuuuxx |||| s|||||||| ||und d</controlfield>
                        </xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>

                    <!-- FUNK Tag 010-019 -->
                    <xsl:apply-templates select="marc:datafield[@tag &lt; '020']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Print-isbn -->
                    <xsl:variable name="df776z" select="normalize-space(marc:datafield[@tag = '776'][1]/marc:subfield[@code = 'z'][1])"/>

                    <!-- FUNK Tag 020 -->
                    <xsl:for-each select="marc:datafield[@tag = '020']">
                        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                            <!--<xsl:if test="$has007cr = 'true'">
                                <xsl:for-each select="marc:subfield[@code = 'a' and (contains(., '(e-book)') or contains(., '(ebook)') or contains(., '(eBook)') or contains(., '(electronic)') or contains(., '(ebk'))]">
                                    <subfield code="{@code}">
                                        <xsl:value-of select="normalize-space(.)"/>
                                    </subfield>
                                </xsl:for-each>
                            </xsl:if>-->

                            <xsl:if test="$has007cr != 'true'">
                                <xsl:for-each select="marc:subfield[@code = 'a' and not(contains(., '(e-book)')) and not(contains(., '(ebook)')) and not(contains(., '(eBook)')) and not(contains(., '(electronic)')) and not(contains(., '(ebk'))]">
                                    <subfield code="{@code}">
                                        <xsl:value-of select="normalize-space(.)"/>
                                    </subfield>
                                </xsl:for-each>
                            </xsl:if>

                            <xsl:for-each select="marc:subfield[@code != 'a' and @code != '5' and @code != '9']">
                                <subfield code="{@code}">
                                    <xsl:value-of select="normalize-space(.)"/>
                                </subfield>
                            </xsl:for-each>

                            <xsl:if test="$has007cr = 'true'">
                                <xsl:for-each select="marc:subfield[@code = 'a' and contains(., '(') and not(contains(., '(e-book)')) and not(contains(., '(ebook)')) and not(contains(., '(eBook)')) and not(contains(., '(electronic)')) and not(contains(., '(ebk'))]">
                                    <subfield code="z">
                                        <xsl:value-of select="normalize-space(.)"/>
                                    </subfield>
                                </xsl:for-each>
                                <xsl:for-each select="marc:subfield[@code = 'a' and contains(., '(e-book)') or contains(., '(ebook)') or contains(., '(eBook)') or contains(., '(electronic)') or contains(., '(ebk')]">
                                    <subfield code="a">
                                        <xsl:value-of select="normalize-space(.)"/>
                                    </subfield>
                                </xsl:for-each>
                                <xsl:for-each select="marc:subfield[@code = 'a' and not(contains(., '('))]">
                                    <subfield code="z">
                                        <xsl:value-of select="normalize-space(.)"/>
                                    </subfield>
                                </xsl:for-each>
                            </xsl:if>

                            <xsl:if test="$has007cr != 'true'">
                                <xsl:for-each select="marc:subfield[@code = 'a' and (contains(., '(e-book)') or contains(., '(ebook)') or contains(., '(eBook)') or contains(., '(electronic)') or contains(., '(ebk'))]">
                                    <subfield code="z">
                                        <xsl:value-of select="normalize-space(.)"/>
                                    </subfield>
                                </xsl:for-each>
                            </xsl:if>

                            <xsl:if test="position() = 1 and $has007cr = 'true' and $df776z != '' and count(marc:subfield[@code = 'z']) = 0  and count(../marc:datafield[@tag = '020']/marc:subfield[@code = 'a'][starts-with(., $df776z)]) = 0">
                                <subfield code="z">
                                    <xsl:value-of select="$df776z"/><xsl:text> (print)</xsl:text>
                                </subfield>
                            </xsl:if>
                        </datafield>
                    </xsl:for-each>

                    <!-- FUNK print-isbn -->
                    <xsl:if test="count(marc:datafield[@tag = '020']) = 0 and $has007cr = 'true' and $df776z != ''">
                        <datafield ind1=" " ind2=" " tag="020">
                            <subfield code="z">
                                <xsl:value-of select="$df776z"/><xsl:text> (print)</xsl:text>
                            </subfield>
                        </datafield>
                    </xsl:if>

                    <!-- FUNK Tag 021-034 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '020' and @tag &lt; '035']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 035 -->
                    <xsl:apply-templates select="marc:controlfield[@tag = '001']"/>

                    <!-- FUNK Tag 035-039 -->
                    <xsl:apply-templates select="marc:datafield[@tag >= '035' and @tag &lt; '040']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 040 -->
                    <xsl:choose>
                        <xsl:when test="count(marc:datafield[@tag='040'])=0">
                            <datafield ind1=" " ind2=" " tag="040">
                                <subfield code="d">
                                    <xsl:value-of select="marc:datafield[@tag='948']/marc:subfield[@code='l']"/>
                                </subfield>
                                <xsl:if test="$df856u_has_dawsonera = 'true'">
                                    <subfield code="9">Dawsonera</subfield>
                                </xsl:if>
                            </datafield>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates select="marc:datafield[@tag = '040']"><xsl:with-param name="add_dawsonera" select="$df856u_has_dawsonera"/></xsl:apply-templates>
                        </xsl:otherwise>
                    </xsl:choose>

                    <!-- FUNK Tag 041-080 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '040' and @tag &lt; '081']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Datafield 941 transformation to datafield 080 -->
                    <xsl:for-each select="marc:datafield[@tag = '941' and marc:subfield[last()]/@code='2' and normalize-space(marc:subfield[last()])='udc' and normalize-space(marc:subfield[@code='a'])]">
                        <datafield ind1=" " ind2=" " tag="080">
                            <subfield code="a">
                                <xsl:value-of select="marc:subfield[@code='a']"/>
                            </subfield>
                        </datafield>
                    </xsl:for-each>

                    <xsl:apply-templates select="marc:datafield[@tag = '082']" mode="copying-datafields"/>

                    <!-- FUNK Datafield 941 transformation to datafield 082 -->
                    <xsl:for-each select="marc:datafield[@tag = '941' and marc:subfield[last()]/@code='2' and normalize-space(marc:subfield[last()])='ddc' and normalize-space(marc:subfield[@code='a'])]">
                        <datafield ind1="0" ind2=" " tag="082">
                            <subfield code="a">
                                <xsl:value-of select="marc:subfield[@code='a']"/>
                            </subfield>
                        </datafield>
                    </xsl:for-each>

                    <xsl:apply-templates select="marc:datafield[@tag = '084']" mode="copying-datafields"/>

                    <!-- FUNK Datafield 941 transformation to datafield 084 -->
                    <xsl:for-each select="marc:datafield[@tag = '941' and count(marc:subfield) > 1 and normalize-space(marc:subfield[last()])!='udc'
                    and normalize-space(marc:subfield[last()])!='ddc' and normalize-space(marc:subfield[@code='a'])]">
                        <datafield ind1=" " ind2=" " tag="084">
                            <xsl:for-each select="marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                            </xsl:for-each>
                        </datafield>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag = '941' and marc:subfield[1][@code='a'] and count(marc:subfield)=1]">
                        <datafield ind1=" " ind2=" " tag="084">
                            <subfield code="a">
                                <xsl:value-of select="marc:subfield[@code='a']"/>
                            </subfield>
                            <subfield code="2">
                                <xsl:text>kssb/8</xsl:text>
                            </subfield>
                        </datafield>
                    </xsl:for-each>

                    <!-- FUNK Tag 085-089 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '084' and @tag &lt; '090']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- datafield 100, 110, 111, 130 transformation -->
                    <xsl:choose>
                        <xsl:when test="count(marc:datafield[@tag='040'])=0">
                            <xsl:call-template name="tag943rule">
                                <xsl:with-param name="compareTag" select="marc:datafield[@tag = '100' or @tag = '110' or @tag = '111' or @tag = '130'][1]"/>
                                <xsl:with-param name="tag943" select="marc:datafield[@tag='943']"/>
                                <xsl:with-param name="isUsed" select="'false'"/>
                            </xsl:call-template>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:for-each select="marc:datafield[@tag >= '100' and @tag &lt;= '130']">
                                <xsl:call-template name="tag943rule">
                                    <xsl:with-param name="compareTag" select="."/>
                                    <xsl:with-param name="tag943" select="../marc:datafield[@tag='943']"/>
                                    <xsl:with-param name="isUsed" select="'false'"/>
                                </xsl:call-template>
                            </xsl:for-each>
                        </xsl:otherwise>
                    </xsl:choose>

                    <!-- FUNK Tag 131-239 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '130' and @tag &lt; '240']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- datafield tag 240 transformation -->
                    <xsl:for-each select="marc:datafield[@tag = '240']">
                        <xsl:call-template name="tag943rule">
                            <xsl:with-param name="compareTag" select="."/>
                            <xsl:with-param name="tag943" select="../marc:datafield[@tag='943']"/>
                            <xsl:with-param name="isUsed" select="'false'"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <!-- Ändrat 7 juni 2007 (245 har fått en egen regel) -->
                    <!-- FUNK Tag 241-244 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '240' and @tag &lt; '245']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 245 -->
                    <xsl:apply-templates select="marc:datafield[@tag = '245']" mode="ind-check"><xsl:with-param name="cf07_check" select="$has007cr"/></xsl:apply-templates>

                    <!-- FUNK Tag 246-259 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '245' and @tag &lt; '260']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>
                    <!-- Slut ändrat -->

                    <!-- FUNK Tag 260 -->
                    <xsl:apply-templates select="marc:datafield[@tag = '260']"/>

                    <!-- FUNK Tag 261-589 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '260' and @tag &lt; '590']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 599 (add) -->
                    <datafield ind1=" " ind2=" " tag="599">
                        <subfield code="a">Dawson<xsl:if test="marc:datafield[@tag = '947' and marc:subfield[@code = 'a' and (contains(normalize-space(.), 'Updated') or contains(normalize-space(.), 'Upgraded'))]]"><xsl:text>. </xsl:text><xsl:value-of select="marc:datafield[@tag = '947' and marc:subfield[@code = 'a' and (contains(normalize-space(.), 'Updated') or contains(normalize-space(.), 'Upgraded'))]]"/></xsl:if></subfield>
                    </datafield>

                    <!-- datafield tag 600 - tag 630 transformation -->
                    <xsl:for-each select="marc:datafield[@tag >= '600' and @tag &lt;= '630']">
                        <xsl:call-template name="tag943rule">
                            <xsl:with-param name="compareTag" select="."/>
                            <xsl:with-param name="tag943" select="../marc:datafield[@tag='943']"/>
                            <xsl:with-param name="isUsed" select="'false'"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <!-- FUNK Tag 631-650 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '630' and @tag &lt; '651']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 942 -->
                    <xsl:for-each select="marc:datafield[@tag = '942']">
                        <xsl:choose>
                            <xsl:when test="count(marc:subfield[@code='2'])=0 and count(marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
                                <datafield ind1=" " ind2="7" tag="650">
                                    <xsl:for-each select="marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                                        <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                    </xsl:for-each>
                                    <subfield code="2">
                                        <xsl:text>sao</xsl:text>
                                    </subfield>
                                </datafield>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:if test="marc:subfield[last()]/@code='2' and count(marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
                                    <datafield ind1=" " ind2="7" tag="650">
                                        <xsl:for-each select="marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                                            <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                        </xsl:for-each>
                                    </datafield>
                                </xsl:if>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:for-each>

                    <!-- FUNK Tag 651-699 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '650' and @tag &lt; '700']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- datafield tag 700 - tag 730 transformation -->
                    <xsl:for-each select="marc:datafield[@tag >= '700' and @tag &lt;= '730']">
                        <xsl:call-template name="tag943rule">
                            <xsl:with-param name="compareTag" select="."/>
                            <xsl:with-param name="tag943" select="../marc:datafield[@tag='943']"/>
                            <xsl:with-param name="isUsed" select="'false'"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <!-- FUNK Tag 731-775 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '730' and @tag &lt; '776']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 776 (2008-01-14) -->
                    <xsl:choose>
                        <xsl:when test="$has007cr">
                            <xsl:apply-templates select="marc:datafield[@tag = '776']" mode="e-resource">
                                <xsl:with-param name="df100ab">
                                    <xsl:value-of select="normalize-space(marc:datafield[@tag = '100']/marc:subfield[@code = 'a'])"/><xsl:value-of select="normalize-space(marc:datafield[@tag = '100']/marc:subfield[@code = 'b'])"/>
                                </xsl:with-param>
                                <xsl:with-param name="df245a">
                                    <xsl:value-of select="normalize-space(marc:datafield[@tag = '245']/marc:subfield[@code = 'a'])"/>
                                </xsl:with-param>
                                <xsl:with-param name="df260c">
                                    <xsl:value-of select="normalize-space(marc:datafield[@tag = '260'][1]/marc:subfield[@code = 'c'][1])"/>
                                </xsl:with-param>
                            </xsl:apply-templates>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates select="marc:datafield[@tag = '776']" mode="copying-datafields"/>
                        </xsl:otherwise>
                    </xsl:choose>

                    <!-- FUNK Tag 777-849 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '776' and @tag &lt; '850']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- FUNK Tag 856 -->
                    <xsl:apply-templates select="marc:datafield[@tag = '856' and count(marc:subfield[@code = 'u'][contains(., 'http://www.dawsonera.com/')]) = 0]" mode="copying-datafields"/>

                    <!-- FUNK Tag 880-899 -->
                    <xsl:apply-templates select="marc:datafield[@tag > '879' and @tag &lt; '900']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>

                    <!-- datafield tag 886 transformation - not needed if sort works -->
                    <!--
                    <xsl:for-each select="marc:datafield[@tag = '943']">
                    <xsl:call-template name="tag886rule">
                    <xsl:with-param name="compTag" select="../marc:datafield[(@tag >= '100' and @tag &lt;= '130') or @tag='240' or (@tag >= '600' and @tag &lt;= '630') or (@tag >= '700' and @tag &lt;= '730')]"/>
                    </xsl:call-template>
                    </xsl:for-each>
                    -->

                    <!-- datafield tag 887 - tag 899 transformation -->
                    <!--<xsl:apply-templates select="marc:datafield[@tag > '886' and @tag &lt; '900']" mode="copying-datafields">
                        <xsl:sort select="@tag"/>
                    </xsl:apply-templates>-->

                    <!-- datafield tag 943 transformation -->
                    <xsl:apply-templates select="marc:datafield[@tag = '943']" mode="copying-datafields"/>

                </record>

                <!-- holdings record -->
                <record type="Holdings">
                    <!-- leader -->
                    <leader>#####nx  a22#####1n 4500</leader>

                    <!-- 008 -->
                    <xsl:variable name="cf008"><xsl:value-of select="marc:controlfield[@tag='008']"/></xsl:variable>
                    <controlfield tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||001||||||000000</controlfield>
                    
                    <!-- 001 -->
                    <xsl:for-each select="marc:controlfield[@tag='001']">
                        <datafield ind1=" " ind2=" " tag="035">
                            <subfield code="a">
                                <xsl:value-of select="."/>
                            </subfield>
                        </datafield>
                    </xsl:for-each>

                    <!-- 948 -->
                    <xsl:for-each select="marc:datafield[@tag='948']">
                        <xsl:if test="count(marc:subfield[@code = 'l' or @code = 's'][normalize-space(.)]) > '0'">
                            <datafield ind1=" " ind2=" " tag="852">
                                <xsl:if test="count(marc:subfield[@code = 'l']) > '0'">
                                    <subfield code="b">
                                        <xsl:value-of select="normalize-space(marc:subfield[@code='l'])"/>
                                    </subfield>
                                </xsl:if>
                                <xsl:if test="count(marc:subfield[@code = 's']) > '0'">
                                    <subfield code="h">
                                        <xsl:value-of select="normalize-space(marc:subfield[@code='s'])"/>
                                    </subfield>
                                </xsl:if>
                                <xsl:if test="../marc:datafield[@tag='951' and normalize-space(marc:subfield[@code='a'])]">
                                    <subfield code="z">
                                        <xsl:value-of select="../marc:datafield[@tag='951' and normalize-space(marc:subfield[@code='a'])][1]/marc:subfield[@code='a']"/>
                                    </subfield>
                                </xsl:if>
                                <!--<xsl:if test="../marc:datafield[@tag='951' and marc:subfield[@code='b']]">
                                    <subfield code="x">
                                        <xsl:value-of select="../marc:datafield[@tag='951' and marc:subfield[@code='b']][1]/marc:subfield[@code='b']"/>
                                    </subfield>
                                </xsl:if>-->
                            </datafield>
                        </xsl:if>
                   </xsl:for-each>

                    <!-- datafield tag 856 transformation -->
                    <xsl:apply-templates select="marc:datafield[@tag = '856' and count(marc:subfield[@code = 'u'][contains(., 'http://www.dawsonera.com/')]) > 0]" mode="dawsonera"/>

                    <!-- 948 -->
                    <xsl:for-each select="marc:datafield[@tag='948']">
                        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                            <xsl:for-each select="marc:subfield[@code != '5' and @code != '9']">
                                <subfield code="{@code}">
                                    <xsl:value-of select="normalize-space(.)"/>
                                </subfield>
                            </xsl:for-each>
                        </datafield>
                    </xsl:for-each>

                    <!-- 951 -->
                    <xsl:apply-templates select="marc:datafield[@tag = '951']" mode="copying-datafields"/>
                </record>
            </xsl:when>
            <xsl:otherwise>
                <xsl:message><xsl:text>Dawson: No sigel for </xsl:text><xsl:value-of select="marc:controlfield[@tag = '001']"/></xsl:message>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- FUNK Leader -->
    <xsl:template match="marc:leader">
        <xsl:param name="isPreliminary"/>
        <!--#  #  #  #  #  n  a  m     a  2  2  #  #  #  #  #  8  a     4  5  0  0   -->
        <!--1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 -->
        <!--0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23 -->
        <xsl:variable name="leader" select="."/>
        <xsl:variable name="tmp5-8" select="substring($leader,6,4)"/>
        <xsl:variable name="tmp17" select="substring($leader,18,1)"/>
        <xsl:variable name="tmp18" select="substring($leader,19,1)"/>
        <xsl:variable name="tmp19" select="substring($leader,20,1)"/>
        <xsl:variable name="leader5-8" select="translate($tmp5-8,'-',' ')"/>
        <xsl:variable name="leader17" select="translate($tmp17,'-',' ')"/>
        <xsl:variable name="leader18" select="translate($tmp18,'-',' ')"/>
        <xsl:variable name="leader19" select="translate($tmp19,'-',' ')"/>
        <xsl:variable name="replace17">
            <xsl:choose>
                <xsl:when test="$isPreliminary = 'true'">5</xsl:when>
                <xsl:when test="$leader17 = ' '">7</xsl:when>
                <xsl:otherwise><xsl:value-of select="$leader17"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="replace18">
            <xsl:choose>
                <xsl:when test="$leader18 = ' '">a</xsl:when>
                <xsl:otherwise><xsl:value-of select="$leader18"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <leader>#####<xsl:value-of select="$leader5-8"/>a22#####<xsl:value-of select="$replace17"/><xsl:value-of select="$replace18"/><xsl:value-of select="$leader19"/>4500</leader>
    </xsl:template>

    <!-- FUNK Controlfield -->
    <xsl:template match="marc:controlfield[@tag != '001']">
        <xsl:param name="has007cr"/>
        <xsl:param name="timeStamp"/>
        <xsl:choose>
            <xsl:when test="@tag = '007' and $has007cr = 'true'">
                <controlfield tag="007">cr||||||||||||</controlfield>
            </xsl:when>
            <xsl:when test="@tag = '008' and normalize-space(substring(.,1,6)) = ''">
                <xsl:variable name="afterDate" select="substring(.,7,34)"/>
                <controlfield tag="008"><xsl:value-of select="$timeStamp"/><xsl:value-of select="$afterDate"/></controlfield>
            </xsl:when>
            <xsl:otherwise>
                <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- FUNK Controlfield tag = 001 -->
    <xsl:template match="marc:controlfield[@tag = '001']">
        <datafield tag="035" ind1=" " ind2=" ">
            <subfield code="a"><xsl:value-of select="."/></subfield>
        </datafield>
    </xsl:template>

    <!-- FUNK Copying datafields - general rules -->
    <xsl:template match="*" mode="copying-datafields">
        <xsl:if test="count(marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
            <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                <xsl:for-each select="marc:subfield[@code != '5' and @code != '9']">
                    <subfield code="{@code}">
                        <xsl:value-of select="normalize-space(.)"/>
                    </subfield>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>

     <!-- FUNK Holdings 856 with z Dawsonera -->
    <xsl:template match="marc:datafield[@tag = '856']" mode="dawsonera">
        <xsl:if test="count(marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
            <datafield ind1="{@ind1}" ind2="{@ind2}" tag="856">
                <xsl:for-each select="marc:subfield[@code != '5' and @code != '9']">
                    <subfield code="{@code}">
                        <xsl:value-of select="normalize-space(.)"/>
                    </subfield>
                    <subfield code="z">Dawsonera</subfield>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>

    <!-- FUNK Tag 040 -->
    <xsl:template match="marc:datafield[@tag = '040']">
        <xsl:param name="add_dawsonera"/>
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:for-each select="marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
            <xsl:if test="../marc:datafield[@tag='948']/marc:subfield[@code='l'][normalize-space(.)]">
                <subfield code="d">
                    <xsl:value-of select="../marc:datafield[@tag='948']/marc:subfield[@code='l']"/>
                </subfield>
            </xsl:if>
            <xsl:if test="$add_dawsonera = 'true'">
                <subfield code="9">Dawsonera</subfield>
            </xsl:if>
        </datafield>
    </xsl:template>

    <!-- FUNK Tag = 245 (ny 7 juni 2007, modifierad 14 januari 2008) -->
    <xsl:template match="marc:datafield[@tag = '245']" mode="ind-check">
        <xsl:param name="cf07_check"/>
        <xsl:variable name="i1">
            <xsl:choose>
                <xsl:when test="@ind1 = '' or @ind1 = ' '">1</xsl:when>
                <xsl:otherwise><xsl:value-of select="@ind1"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="i2">
            <xsl:choose>
                <xsl:when test="@ind2 = '' or @ind2 = ' '">0</xsl:when>
                <xsl:otherwise><xsl:value-of select="@ind2"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="count(marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
            <datafield ind1="{$i1}" ind2="{$i2}" tag="245">
                <xsl:for-each select="marc:subfield[@code != '5' and @code != '9']">
                    <xsl:choose>
                        <xsl:when test="@code = 'h' and $cf07_check = 'true'">
                            <xsl:variable name="interp">
                                <xsl:choose>
                                    <xsl:when test="contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'), '[electronic resource]')"><xsl:value-of select="substring-after(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'), '[electronic resource]')"/></xsl:when>
                                    <xsl:otherwise><xsl:value-of select="."/></xsl:otherwise>
                                </xsl:choose>
                            </xsl:variable>
                            <subfield code="h">[Elektronisk resurs]<xsl:value-of select="$interp"/></subfield>
                        </xsl:when>
                        <xsl:otherwise>
                            <subfield code="{@code}">
                                <xsl:value-of select="normalize-space(.)"/>
                            </subfield>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>

    <!-- FUNK Tag 260 -->
    <xsl:template match="marc:datafield[@tag = '260']">
        <xsl:choose>
            <xsl:when test="count(../marc:datafield[@tag='040']) = 0 and count(marc:subfield[normalize-space(.)]) > '0'">
                <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                    <xsl:if test="marc:subfield[@code='a'][normalize-space(.)]">
                        <subfield code="a"><xsl:value-of select="marc:subfield[@code='a'][normalize-space(.)][1]"/></subfield>
                    </xsl:if>
                    <xsl:if test="marc:subfield[@code='b'][normalize-space(.)]">
                        <subfield code="b"><xsl:value-of select="marc:subfield[@code='b'][normalize-space(.)][1]"/></subfield>
                    </xsl:if>
                    <xsl:if test="marc:subfield[@code='c'][normalize-space(.)]">
                        <subfield code="c"><xsl:value-of select="marc:subfield[@code='c'][normalize-space(.)][1]"/></subfield>
                    </xsl:if>
                </datafield>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates select="." mode="copying-datafields"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- FuNK Tag 776 (för e-böcker, nytt 14 januari 2008) -->
    <xsl:template match="marc:datafield[@tag = '776']" mode="e-resource">
        <xsl:param name="df100ab"/>
        <xsl:param name="df245a"/>
        <xsl:param name="df260c"/>
        <datafield ind1="0" ind2="8" tag="776">
            <xsl:if test="marc:subfield[@code = 'c']">
                <subfield code="i"><xsl:value-of select="normalize-space(marc:subfield[@code = 'c'])"/>:</subfield>
            </xsl:if>
            <xsl:if test="$df100ab != ''">
                <subfield code="a"><xsl:value-of select="$df100ab"/></subfield>
            </xsl:if>
            <xsl:if test="$df245a != ''">
                <subfield code="t"><xsl:value-of select="$df245a"/></subfield>
            </xsl:if>
            <xsl:if test="$df260c != ''">
                <subfield code="d"><xsl:value-of select="$df260c"/></subfield>
            </xsl:if>
            <xsl:for-each select="marc:subfield[@code='z'][normalize-space(.)]">
                <subfield code="z"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <!-- Datafield tag = 100,110,111,130,240,600,610,611,630,700,710,711,730 -->
    <xsl:template name="tag943rule">
        <xsl:param name="compareTag"/>
        <xsl:param name="tag943"/>
        <xsl:param name="isUsed"/>

        <!-- Nytt 12 juni 2007: tom @ind1 byts ut mot variabeln @indikator -->
        <xsl:variable name="indikator">
            <xsl:choose>
                <xsl:when test="($compareTag/@tag = '100' or $compareTag/@tag = '600' or $compareTag/@tag = '700') and $compareTag/@ind1 = ''">1</xsl:when>
                <xsl:when test="($compareTag/@tag = '110' or $compareTag/@tag = '610' or $compareTag/@tag = '710' or $compareTag/@tag = '111' or $compareTag/@tag = '611' or $compareTag/@tag = '711') and $compareTag/@ind1 = ''">2</xsl:when>
                <xsl:otherwise><xsl:value-of select="$compareTag/@ind1"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <!-- Slut nytt -->

        <!-- Nytt 25 april 2008: @ind2 sätts till "4" för df 600, 610, 611, 630 -->
        <xsl:variable name="indikator2">
            <xsl:choose>
                <xsl:when test="$compareTag/@tag = '600' or $compareTag/@tag = '610' or $compareTag/@tag = '611' or $compareTag/@tag = '630'">4</xsl:when>
                <xsl:otherwise><xsl:value-of select="$compareTag/@ind2"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <!-- Slut nytt -->

        <xsl:if test="$isUsed='false'">
            <xsl:choose>
                <!-- If 943 exists and has two subfields @a, compare subfields without punctuation marks and store eventual mismatch in variable $comparing -->
                <xsl:when test="$tag943">
                    <xsl:choose>
                        <xsl:when test="count($tag943[1]/marc:subfield[@code = 'a']) > 1">
                            <xsl:variable name="comparing">
                            <xsl:variable name="matchTest"/>
                                <xsl:for-each select="$tag943[1]/marc:subfield[@code = 'a'][2] | $tag943[1]/marc:subfield[@code = 'a'][2]/following-sibling::marc:subfield">
                                    <xsl:variable name="pos" select="position()"/>
                                    <xsl:variable name="compTemp1" select="normalize-space(.)"/>
                                    <xsl:variable name="compTemp2" select="translate($compTemp1,'.','')"/>
                                    <xsl:variable name="compTemp3" select="translate($compTemp2,',','')"/>
                                    <xsl:variable name="shortTemp1" select="normalize-space($compareTag/marc:subfield[$pos])"/>
                                    <xsl:variable name="shortTemp2" select="translate($shortTemp1,'.','')"/>
                                    <xsl:variable name="shortTemp3" select="translate($shortTemp2,',','')"/>
                                    <xsl:if test="$compTemp3 != $shortTemp3 or /@code != $compareTag/marc:subfield[$pos]/@code">
                                        <xsl:value-of select="concat($matchTest,'noMatch')"/>
                                    </xsl:if>
                                </xsl:for-each>
                            </xsl:variable>

                            <!-- If the concatenated variable contains the string 'noMatch', the node sets are unequal. In that case,
                            output the content of datafield tag="100/700" etc. -->
                            <xsl:choose>
                                <xsl:when test="contains($comparing,'noMatch')">
                                    <xsl:choose>
                                        <xsl:when test="count($tag943[2]) = 0">
                                            <xsl:if test="count($compareTag/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
                                                <!-- Ändring 12 juni 2007: @ind1 byts ut mot variabeln $indikator -->
                                                <!--<datafield ind1="{$compareTag/@ind1}" ind2="{$compareTag/@ind2}" tag="{$compareTag/@tag}">-->
                                                <datafield ind1="{$indikator}" ind2="{$indikator2}" tag="{$compareTag/@tag}">
                                                    <!-- Slut ändring -->
                                                    <xsl:for-each select="$compareTag/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                                                        <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                                    </xsl:for-each>
                                                </datafield>
                                            </xsl:if>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:call-template name="tag943rule">
                                                <xsl:with-param name="compareTag" select="$compareTag"/>
                                                <xsl:with-param name="tag943" select="$tag943[position() > 1]"/>
                                                <xsl:with-param name="isUsed" select="'false'"/>
                                            </xsl:call-template>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:when>

                                <!-- Otherwise, output 943:s content and call template recursively if there are more 943 datafields -->
                                <xsl:otherwise>
                                    <xsl:if test="count($tag943[1]/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
                                        <!-- Ändring 12 juni 2007: @ind1 byts ut mot variabeln @indikator -->
                                        <!--<datafield ind1="{$compareTag/@ind1}" ind2="{$compareTag/@ind2}" tag="{$compareTag/@tag}">-->
                                        <datafield ind1="{$indikator}" ind2="{$indikator2}" tag="{$compareTag/@tag}">
                                            <!-- Slut ändring -->
                                            <xsl:for-each select="$tag943[1]/marc:subfield[@code = 'a'][2]/preceding-sibling::marc:subfield[normalize-space(.)]">
                                                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                            </xsl:for-each>
                                        </datafield>
                                        <datafield ind1="2" ind2=" " tag="886">
                                            <subfield code="a"><xsl:value-of select="$compareTag/@tag"/></subfield>
                                            <subfield code="b"><xsl:value-of select="$compareTag/@ind1"/><xsl:value-of select="$compareTag/@ind2"/></subfield>
                                            <xsl:for-each select="$tag943[1]/marc:subfield[@code != '5' and @code != '9']">
                                                <subfield code="{@code}">
                                                    <xsl:value-of select="normalize-space(.)"/>
                                                </subfield>
                                            </xsl:for-each>
                                        </datafield>
                                    </xsl:if>
                                    <xsl:if test="count($tag943[2]) > 0">
                                        <xsl:call-template name="tag943rule">
                                            <xsl:with-param name="compareTag" select="$compareTag"/>
                                            <xsl:with-param name="tag943" select="$tag943[position() > 1]"/>
                                            <xsl:with-param name="isUsed" select="'true'"/>
                                        </xsl:call-template>
                                    </xsl:if>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>

                        <!-- If 943 has only one subfield @a, output 100/700 etc -->
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="count($tag943[2]) = 0">
                                    <xsl:if test="count($compareTag/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
                                        <!-- Ändring 12 juni 2007: @ind1 byts ut mot variabeln @indikator -->
                                        <!--<datafield ind1="{$compareTag/@ind1}" ind2="{$compareTag/@ind2}" tag="{$compareTag/@tag}">-->
                                        <datafield ind1="{$indikator}" ind2="{$indikator2}" tag="{$compareTag/@tag}">
                                            <!-- Slut ändring -->
                                            <xsl:for-each select="$compareTag/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                                                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                            </xsl:for-each>
                                        </datafield>
                                    </xsl:if>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:call-template name="tag943rule">
                                        <xsl:with-param name="compareTag" select="$compareTag"/>
                                        <xsl:with-param name="tag943" select="$tag943[position() > 1]"/>
                                        <xsl:with-param name="isUsed" select="'false'"/>
                                    </xsl:call-template>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <!-- If there is no 943 at all, output 100/700 etc -->
                <xsl:otherwise>
                    <xsl:if test="count($compareTag/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]) > '0'">
                        <!-- Ändring 12 juni 2007: @ind1 byts ut mot variabeln @indikator -->
                        <!--<datafield ind1="{$compareTag/@ind1}" ind2="{$compareTag/@ind2}" tag="{$compareTag/@tag}">-->
                        <datafield ind1="{$indikator}" ind2="{$indikator2}" tag="{$compareTag/@tag}">
                            <!-- Slut ändring -->
                            <xsl:for-each select="$compareTag/marc:subfield[@code != '5' and @code != '9'][normalize-space(.)]">
                                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                            </xsl:for-each>
                        </datafield>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>

    <!-- KORR! Proxy-prefix i dawsonera-länken (AVVAKTA) -->
    <!--<xsl:call-template name="create-holding"><xsl:with-param name="sigel" select="'H'"/><xsl:with-param name="proxy" select="'https://www04.sub.su.se/login?url=http://uraccess.navicast.net/mov.php?xid='"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Endast tillgänglig inom SU:s nät'"/></xsl:call-template>-->
    <!--<xsl:template name="create-holding">
        <xsl:param name="sigel"/>
        <xsl:param name="proxy"/>
        <xsl:param name="id"/>
        <xsl:param name="comment"/>
        -->
    <!-- 856 -->
    <!--    <datafield ind1="4" ind2="0" tag="856">
                <subfield code="u"><xsl:value-of select="concat($proxy, $id)"/></subfield>
                <subfield code="z"><xsl:value-of select="$comment"/></subfield>
            </datafield>
        </record>
    </xsl:template>-->
</xsl:stylesheet>


