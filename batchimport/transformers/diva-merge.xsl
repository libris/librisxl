<?xml version="1.0" encoding="MacRoman"?>

<!--
    Document   : diva-merge.xsl
    Created on : January 20, 2010, 5:11 PM
    Author     : pelle
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>

    <xsl:template match="/">
        <xsl:apply-templates select="merge"/>
    </xsl:template>

    <xsl:template match="merge">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:variable name="leader"><xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/></xsl:variable>
            <xsl:variable name="leader17" select="substring($leader,18,1)"/>

            <xsl:variable name="post_to_import">
                <xsl:choose>
                    <!-- SFX-VILLKORET DISSAT TILLS VIDARE -->
                    <!--<xsl:when test="$isSfx = 'yes' and $leader17 = '5'">new</xsl:when>-->
                    <xsl:when test="$leader17 = '5' or $leader17 = '8'">new</xsl:when>
                    <xsl:otherwise>old</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <!--<testing><xsl:value-of select="$isSfx"/>, <xsl:value-of select="$leader17"/>, <xsl:value-of select="$post_to_import"/></testing>-->
            <xsl:choose>
                <xsl:when test="$post_to_import = 'old'">
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
                        <!-- old leader transformation -->
                        <leader><xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/></leader>

                        <!-- old controlfield transformation -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:controlfield">
                            <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>
                        </xsl:for-each>

                        <!-- old datafield transformation -->
                        <datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DiVA)<xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:controlfield[@tag = '001']"/></subfield></datafield>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield" mode="copying-datafields"/>

                    </record>
                </xsl:when>
                <xsl:otherwise>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
                        <!-- new leader transformation -->
                        <leader><xsl:value-of select="new_record/marc:record[@type='Bibliographic']/marc:leader"/></leader>

                        <!-- new controlfield transformation -->
                        <xsl:for-each select="new_record/marc:record[@type='Bibliographic']/marc:controlfield">
                            <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>
                        </xsl:for-each>

                        <!-- new datafield transformation -->   
                        <datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DiVA)<xsl:value-of select="new_record/marc:record[@type='Bibliographic']/marc:controlfield[@tag = '001']"/></subfield></datafield>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield" mode="copying-datafields"/>
                    </record>
                </xsl:otherwise>
            </xsl:choose>
        </collection>
    </xsl:template>

    <!-- Copying datafields - general rules -->
    <xsl:template match="*" mode="copying-datafields">
        <xsl:if test="count(marc:subfield[normalize-space(.)]) > '0'">
            <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                <xsl:for-each select="marc:subfield[normalize-space(.)]">
                    <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>

</xsl:stylesheet>


