<?xml version="1.0" encoding="UTF-8" ?>
<!--
    Document   : bibtex.xsl
    Created on : Aug 10, 2007, 8:15 PM
    Author     : per aberg
    Description:
        Transformation from MarcXML to BibTex format
-->
<xsl:stylesheet
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:marc="http://www.loc.gov/MARC21/slim"
        version="1.0">
    <xsl:output method="text" encoding="UTF-8" indent="no"/>
    <xsl:template match="/search">
        <xsl:apply-templates select="documents"/>
    </xsl:template>

    <xsl:template match="documents">
        <xsl:apply-templates select="document"/>
    </xsl:template>

    <xsl:template match="document">
        <xsl:apply-templates select="paragraph[@name='wmrc']/marc:record"/>
    </xsl:template>

    <!-- RECORD TEMPLATE -->
    <xsl:template match="marc:record">
        <xsl:variable name="leader_7"><xsl:value-of select="substring(marc:leader,8,1)"/></xsl:variable>

        <!-- Reference Type -->
        <xsl:variable name="type"><xsl:call-template name="record-type"/></xsl:variable>
        <xsl:value-of select="concat('RT ', $type, '&#xd;&#xa;')"/>

        <!-- Reference Identifier -->
        <xsl:variable name="id" select="normalize-space(marc:controlfield[@tag = '001'])"/>
        <xsl:if test="normalize-space($id)">
            <xsl:value-of select="concat('ID ', normalize-space($id), '&#xd;&#xa;')"/>
        </xsl:if>

        <!-- Primary Authors -->
        <xsl:apply-templates select="marc:datafield[@tag = '100']"><xsl:with-param name="tagname" select="'A1'"/></xsl:apply-templates>

        <xsl:if test="$leader_7 != 's'">
            <xsl:if test="marc:datafield[@tag = '100']">
                <xsl:apply-templates select="marc:datafield[@tag = '700' and not(marc:subfield[@code = 't']) and not(marc:subfield[@code = '4'])]"><xsl:with-param name="tagname" select="'A1'"/></xsl:apply-templates>
            </xsl:if>
            <xsl:apply-templates select="marc:datafield[@tag = '700' and not(marc:subfield[@code = 't']) and normalize-space(translate(marc:subfield[@code = '4'], 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')) = 'AUT']"><xsl:with-param name="tagname" select="'A1'"/></xsl:apply-templates>
        </xsl:if>

        <!-- Primary Title -->
        <xsl:apply-templates select="marc:datafield[@tag = '222']"/>
        <xsl:if test="marc:datafield[@tag = '245'] and not(marc:datafield[@tag = '222'])">
            <xsl:apply-templates select="marc:datafield[@tag = '245']"/>
        </xsl:if>

        <!-- Periodical Full -->
        <xsl:if test="$leader_7 = 'a' or $leader_7 = 'b'">
            <xsl:apply-templates select="marc:datafield[@tag = '773']" mode="perfull"/>
        </xsl:if>

        <!-- Publication Year -->
        <xsl:variable name="year" select="substring(marc:controlfield[@tag = '008'], 8, 4)"/>
        <xsl:if test="normalize-space($year)">
            <xsl:value-of select="concat('YR ', normalize-space($year), '&#xd;&#xa;')"/>
        </xsl:if>

        <!-- Volume -->
        <xsl:if test="$type = 'Book, Section'">
            <xsl:apply-templates select="marc:datafield[@tag = '773']" mode="volume"/>
        </xsl:if>

        <!-- Issue -->
        <!--<xsl:apply-templates select="marc:datafield[@tag = '773']" mode="issue"/>-->

        <!-- Abstract -->
        <xsl:apply-templates select="marc:datafield[@tag = '520' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '6'])]"/>

        <!-- Notes -->
        <xsl:apply-templates select="marc:datafield[@tag = '502']"/>

        <!-- Keywords -->
        <xsl:apply-templates select="marc:datafield[(@tag = '600' or @tag = '610') and not(marc:subfield[@code = '5'])]"/>
        <xsl:apply-templates select="marc:datafield[@tag = '611' and not(marc:subfield[@code = '5'])]"/>
        <xsl:apply-templates select="marc:datafield[@tag = '630' and not(marc:subfield[@code = '5'])]"/>
        <xsl:apply-templates select="marc:datafield[@tag = '650' and marc:subfield[@code = '2'][. = 'sao'] and not(marc:subfield[@code = '5'])]"/>
        <xsl:apply-templates select="marc:datafield[@tag = '651' and not(marc:subfield[@code = '5'])]"/>

        <!-- Secondary Authors -->
        <xsl:if test="$leader_7 != 's'">
            <xsl:if test="not(marc:datafield[@tag = '100'])">
                <xsl:apply-templates select="marc:datafield[@tag = '700' and not(marc:subfield[@code = 't']) and not(marc:subfield[@code = '4'])]"><xsl:with-param name="tagname" select="'A2'"/></xsl:apply-templates>
            </xsl:if>
            <xsl:apply-templates select="marc:datafield[@tag = '700' and not(marc:subfield[@code = 't']) and marc:subfield[@code = '4'][. = 'edt' or . = 'oth' or . = 'pbl' or . = 'rpy' or . = 'pbd' or . = 'EDT' or . = 'OTH' or . = 'PBL' or . = 'RPY' or . = 'PBD']]"><xsl:with-param name="tagname" select="'A2'"/></xsl:apply-templates>
        </xsl:if>

        <!-- Edition, Publisher, Place of Publication -->
        <xsl:choose>
            <xsl:when test="$leader_7 = 'a' or $leader_7 = 'b'">
                <xsl:apply-templates select="marc:datafield[@tag = '773']" mode="edinfo"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates select="marc:datafield[@tag = '250']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '260' or @tag = '264']"/>
            </xsl:otherwise>
        </xsl:choose>

        <!-- Quaternary Authors -->
        <xsl:if test="$leader_7 != 's'">
            <xsl:apply-templates select="marc:datafield[@tag = '700' and not(marc:subfield[@code = 't']) and marc:subfield[@code = '4'][normalize-space(translate(., 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')) = 'TRL']]"><xsl:with-param name="tagname" select="'A4'"/></xsl:apply-templates>
            <xsl:apply-templates select="marc:datafield[@tag = '700' and not(marc:subfield[@code = 't']) and marc:subfield[@code = '4'][normalize-space(translate(., 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')) = 'COM']]"><xsl:with-param name="tagname" select="'A5'"/></xsl:apply-templates>
        </xsl:if>

        <!-- ISSN/ISBN -->
        <xsl:choose>
            <xsl:when test="$leader_7 = 'a' or $leader_7 = 'b'">
                <xsl:apply-templates select="marc:datafield[@tag = '773']" mode="isxn"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates select="marc:datafield[@tag = '020' or @tag = '022']"/>
            </xsl:otherwise>
        </xsl:choose>

        <!-- URL -->
        <xsl:apply-templates select="marc:datafield[@tag = '856' and not(marc:subfield[@code = '5'])]"/>
        <xsl:value-of select="'&#xd;&#xa;'"/>
    </xsl:template>

    <!-- DATAFIELD TEMPLATES -->
    <!-- Primary Authors -->
    <xsl:template match="marc:datafield[@tag = '100' or @tag = '700']">
        <xsl:param name="tagname"/>
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat($tagname, ' ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Primary Title -->
    <xsl:template match="marc:datafield[@tag='222']">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('T1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='245']">
        <xsl:variable name="ebooking" select="count(../marc:controlfield[@tag = '007' and normalize-space(substring(., 1, 2)) = 'cr'])"/>
        <xsl:variable name="data">
            <xsl:choose>
                <xsl:when test="$ebooking > 0">
                    <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b' or @code = 'f' or @code = 'g' or @code = 'h' or @code = 'n' or @code = 'p' or @code = 's']">
                        <xsl:call-template name="write-subfield"/>
                        <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
                    </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b' or @code = 'f' or @code = 'g' or @code = 'n' or @code = 'p' or @code = 's']">
                        <xsl:call-template name="write-subfield"/>
                        <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
                    </xsl:for-each>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('T1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Periodical Full -->
    <xsl:template match="marc:datafield[@tag='773']" mode="perfull">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 't']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('T2 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Volume -->
    <xsl:template match="marc:datafield[@tag='773']" mode="volume">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'g']">
                <!--<xsl:for-each select="marc:subfield[(@code = 'd' and not(starts-with(., '1')) and not(starts-with(., '2'))) or @code = 't']">-->
                <xsl:choose>
                    <xsl:when test="contains(., ' : ')"><xsl:value-of select="normalize-space(substring-before(., ' : '))"/></xsl:when>
                    <xsl:otherwise><xsl:call-template name="write-subfield"/></xsl:otherwise>
                </xsl:choose>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('VO ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Issue -->
    <xsl:template match="marc:datafield[@tag='773']" mode="issue">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'g']">
                <xsl:choose>
                    <xsl:when test="contains(., ' : ')"><xsl:value-of select="normalize-space(substring-before(., ' : '))"/></xsl:when>
                    <xsl:otherwise><xsl:call-template name="write-subfield"/></xsl:otherwise>
                </xsl:choose>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('IS ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Abstract -->
    <xsl:template match="marc:datafield[@tag='520']">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('AB ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Notes -->
    <xsl:template match="marc:datafield[@tag='502']">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('AB ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Keywords -->
    <xsl:template match="marc:datafield[(@tag='600' or @tag='610') and not(marc:subfield[@code = '5'])]">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b' or @code = 'c' or @code = 'd' or @code = 'f' or @code = 'k' or @code = 'l' or @code = 'm' or @code = 'n' or @code = 'o' or @code = 'p' or @code = 'r' or @code = 's' or @code = 't' or @code = 'v' or @code = 'x' or @code = 'y' or @code = 'z']">
                <xsl:if test="@code = 'v' or @code = 'x' or @code = 'y' or @code = 'z'">-- </xsl:if>
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('K1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='611' and not(marc:subfield[@code = '5'])]">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'c' or @code = 'd' or @code = 'f' or @code = 'k' or @code = 'l' or @code = 'n' or @code = 'p' or @code = 's' or @code = 't' or @code = 'v' or @code = 'x' or @code = 'y' or @code = 'z']">
                <xsl:if test="@code = 'v' or @code = 'x' or @code = 'y' or @code = 'z'">-- </xsl:if>
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('K1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='630' and not(marc:subfield[@code = '5'])]">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'd' or @code = 'f' or @code = 'k' or @code = 'l' or @code = 'm' or @code = 'n' or @code = 'o' or @code = 'p' or @code = 'r' or @code = 's' or @code = 'v' or @code = 'x' or @code = 'y' or @code = 'z']">
                <xsl:if test="@code = 'v' or @code = 'x' or @code = 'y' or @code = 'z'">-- </xsl:if>
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('K1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '650' and marc:subfield[@code = '2'][. = 'sao'] and not(marc:subfield[@code = '5'])]">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b' or @code = 'c' or @code = 'd' or @code = 'v' or @code = 'x' or @code = 'y' or @code = 'z']">
                <xsl:if test="@code = 'v' or @code = 'x' or @code = 'y' or @code = 'z'">-- </xsl:if>
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('K1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='651' and not(marc:subfield[@code = '5'])]">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'v' or @code = 'x' or @code = 'y' or @code = 'z']">
                <xsl:if test="@code = 'v' or @code = 'x' or @code = 'y' or @code = 'z'">-- </xsl:if>
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('K1 ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- Edition, Publisher, Place of Publication -->
    <!-- leader_07 != a, b -->
    <xsl:template match="marc:datafield[@tag='250']">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('ED ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='260' or @tag='264']">
        <xsl:variable name="data1">
            <xsl:for-each select="marc:subfield[@code = 'b']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data1)">
            <xsl:value-of select="concat('PB ', normalize-space($data1), '&#xd;&#xa;')"/>
        </xsl:if>
        <xsl:variable name="data2">
            <xsl:for-each select="marc:subfield[@code = 'a']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data2)">
            <xsl:value-of select="concat('PP ', normalize-space($data2), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- leader_07 = a, b -->
    <xsl:template match="marc:datafield[@tag='773']" mode="edinfo">
        <xsl:variable name="data1">
            <xsl:for-each select="marc:subfield[@code = 'b']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data1)">
            <xsl:value-of select="concat('ED ', normalize-space($data1), '&#xd;&#xa;')"/>
        </xsl:if>
        <xsl:variable name="data2">
            <xsl:for-each select="marc:subfield[(@code = 'd' and not(starts-with(., '1')) and not(starts-with(., '2')))]">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data2)">
            <xsl:value-of select="concat('PB ', normalize-space($data2), '&#xd;&#xa;')"/>
            <!--<xsl:value-of select="concat('PP ', normalize-space($data2), '&#xd;&#xa;')"/>-->
        </xsl:if>
    </xsl:template>

    <!-- ISSN/ISBN -->
    <!-- leader_07 != a, b -->
    <xsl:template match="marc:datafield[@tag='773']" mode="isxn">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'x' or @code = 'z']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('SN ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- leader_07 != a, b -->
    <xsl:template match="marc:datafield[@tag='020' or @tag='022']">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'a']">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('SN ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
    </xsl:template>

    <!-- URL -->
    <xsl:template match="marc:datafield[@tag='856']">
        <xsl:variable name="data">
            <xsl:for-each select="marc:subfield[@code = 'u' or @code = 'y' or @code = 'z' or @code = '3'][starts-with(., 'http://')]">
                <xsl:call-template name="write-subfield"/>
                <xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="normalize-space($data)">
            <xsl:value-of select="concat('UL ', normalize-space($data), '&#xd;&#xa;')"/>
        </xsl:if>
        <xsl:text>&#xd;&#xa;</xsl:text>
    </xsl:template>

    <!-- UTILITY TEMPLATES -->
    <xsl:template name="record-type">
        <xsl:variable name="leader_6"><xsl:value-of select="substring(marc:leader,7,1)"/></xsl:variable>
        <xsl:variable name="leader_7"><xsl:value-of select="substring(marc:leader,8,1)"/></xsl:variable>
        <xsl:variable name="cf007_0-1"><xsl:value-of select="substring(marc:controlfield[@tag = '007'][substring(., 1, 1) = 'c' or substring(., 1, 1) = 'f' or substring(., 1, 1) = 'h'][1], 1, 2)"/></xsl:variable>
        <xsl:variable name="cf008_21"><xsl:value-of select="substring(marc:controlfield[@tag = '008'], 22, 1)"/></xsl:variable>
        <xsl:variable name="cf008_24-27"><xsl:value-of select="substring(marc:controlfield[@tag = '008'],25,3)"/></xsl:variable>
        <xsl:variable name="cf008_29"><xsl:value-of select="substring(marc:controlfield[@tag = '008'],30,1)"/></xsl:variable>
        <xsl:choose>
            <xsl:when test="$leader_6 = 'a' and ($leader_7 = 'a' or $leader_7 = 'b')">Book, Section</xsl:when>
            <xsl:when test="$leader_6 = 'a' and $leader_7 = 'm' and $cf008_29 = '1'">Conference Proceedings</xsl:when>
            <xsl:when test="$leader_6 = 'a' and $leader_7 = 'm' and contains($cf008_24-27, 'm')">Dissertation</xsl:when>
            <xsl:when test="$leader_6 = 'a' and $leader_7 = 'm'">Book, Whole</xsl:when>
            <xsl:when test="$leader_6 = 'a' and $leader_7 = 's' and $cf007_0-1 = 'cr'">Journal, electronic</xsl:when>
            <xsl:when test="$leader_6 = 'a' and $leader_7 = 's'">Journal</xsl:when>
            <xsl:when test="$leader_6 = 'e' or $leader_6 = 'f'">Map</xsl:when>
            <xsl:when test="$leader_6 = 'c' or $leader_6 = 'd'">Music Score</xsl:when>
            <xsl:when test="$leader_6 = 'i' or $leader_6 = 'j'">Sound Recording</xsl:when>
            <xsl:when test="$leader_6 = 'e' or $leader_6 = 'f'">Map</xsl:when>
            <xsl:when test="$leader_6 = 'g'">Video/DVD</xsl:when>
            <xsl:when test="$leader_7 = 'i' and $cf008_21 = 'w'">Web Page</xsl:when>
            <xsl:otherwise>Generic</xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="write-subfield">
        <xsl:variable name="interp" select="substring(., 1 + string-length(.) - 1)"/>
        <xsl:choose>
            <!--<xsl:when test="@code = 'a' and $interp = ':'"><xsl:value-of select="normalize-space(substring(., 1, string-length(.) - 1))"/><xsl:text>: </xsl:text></xsl:when>-->
            <xsl:when test="($interp = '/' or $interp = ':' or $interp = '=' or $interp = '+' or $interp = ',' or $interp = ';') and position() = last()"><xsl:value-of select="normalize-space(substring(., 1, string-length(.) - 1))"/></xsl:when>
            <xsl:when test="position() != last()"><xsl:value-of select="normalize-space(.)"/><xsl:text> </xsl:text></xsl:when>
            <xsl:otherwise><xsl:value-of select="normalize-space(.)"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>


</xsl:stylesheet>
