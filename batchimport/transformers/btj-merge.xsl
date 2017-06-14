<?xml version="1.0" encoding="UTF-8"?>
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
            <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
                <xsl:variable name="leader"><xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/></xsl:variable>
                <xsl:variable name="leader17" select="substring($leader,18,1)"/>
                
                <xsl:choose>
                    <xsl:when test="$leader17 = '5' or $leader17 = '8' or $leader17 = 'u' or $leader17 = 'z' or $leader17 = '|'">
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:leader"/>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:controlfield"/>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag &lt; '240']" mode="copying-datafields"/>
                        <xsl:choose>
                            <xsl:when test="count(new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '240']) > 0">
                                <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '240']" mode="copying-datafields"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '240']" mode="copying-datafields"/>
                            </xsl:otherwise>
                        </xsl:choose>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '240' and @tag &lt; '505']" mode="copying-datafields"/>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '505']" mode="copying-datafields"/>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '505' and @tag &lt; '520']" mode="copying-datafields"/>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '520']" mode="copying-datafields"/>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '520' and @tag &lt; '856']" mode="copying-datafields"/>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '856']" mode="copying-datafields"/>
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '856']" mode="copying-datafields"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:leader"/>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:controlfield"/>
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield" mode="copying-datafields"/>
                    </xsl:otherwise>
                </xsl:choose>
                
            </record>
            
            <xsl:for-each select="old_record/marc:record[@type='Holdings']">
                <record type="Holdings">
                    <xsl:for-each select="marc:datafield">
                        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                            <xsl:for-each select="marc:subfield">
                                <subfield code="{@code}">
                                    <xsl:value-of select="normalize-space(.)"/>
                                </subfield>
                            </xsl:for-each>
                        </datafield>
                    </xsl:for-each>
                </record>
            </xsl:for-each>
            
        </collection>
    </xsl:template>

    <!-- Copying leader -->
    <xsl:template match="marc:leader">
        <leader><xsl:value-of select="."/></leader>
    </xsl:template>
    
    <!-- Copying controlfield -->
    <xsl:template match="marc:controlfield">
        <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>
    </xsl:template>
    
    <!-- Copying datafields - general rules -->
    <xsl:template match="*" mode="copying-datafields">
        <xsl:if test="count(marc:subfield[normalize-space(.)]) > '0'">
            <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                <xsl:for-each select="marc:subfield">
                    <subfield code="{@code}">
                        <xsl:value-of select="normalize-space(.)"/>
                    </subfield>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>
    
</xsl:stylesheet>


