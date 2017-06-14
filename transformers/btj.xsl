<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version="1.0"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
    <xsl:template match="/marc:collection">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="marc:record"/>
        </collection>
    </xsl:template>
    
    <xsl:template match="marc:record">
        <record type="Bibliographic">
            <xsl:apply-templates select="marc:leader"/>
            <xsl:apply-templates select="marc:controlfield[@tag != '001']"/>
            <xsl:apply-templates select="marc:datafield[@tag &lt; '024']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag = '024' and count(marc:subfield[@code = 'a'][starts-with(translate(., 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ'), 'BTJ')]) = 0]" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag > '024' and @tag &lt;= '035']" mode="copying-datafields"/>
            <xsl:if test="marc:controlfield[@tag = '001']">
                <datafield ind1=" " ind2=" " tag="035">
                    <subfield code="a"><xsl:value-of select="marc:controlfield[@tag = '001']"/></subfield>
                </datafield>
            </xsl:if>
            <xsl:apply-templates select="marc:datafield[@tag > '035' and @tag &lt; '084']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag = '084']"/>
            <xsl:apply-templates select="marc:datafield[@tag > '084' and @tag &lt; '100']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag > '099' and @tag &lt; '131' and count(marc:subfield[@code != '0']) > 0]" mode="no_sf-0"/>
            <xsl:apply-templates select="marc:datafield[@tag > '130' and @tag &lt; '240']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag = '240' and count(marc:subfield[@code != '0']) > 0]" mode="no_sf-0"/>
            <xsl:apply-templates select="marc:datafield[@tag > '240' and @tag &lt; '440']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag = '440' and count(marc:subfield[@code != '0' and @code != 'w']) > 0]" mode="to490"/>
            <xsl:apply-templates select="marc:datafield[@tag > '440' and @tag &lt; '508']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag = '508']">
                <xsl:with-param name="leader06" select="substring(marc:leader, 7,1)"/>
            </xsl:apply-templates>
            <xsl:apply-templates select="marc:datafield[@tag > '508' and @tag &lt; '600']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag > '599' and @tag &lt; '663' and count(marc:subfield[@code != '0']) > 0]" mode="no_sf-0"/>
            <xsl:apply-templates select="marc:datafield[@tag > '662' and @tag &lt; '700']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag > '699' and @tag &lt; '755' and count(marc:subfield[@code != '0']) > 0]" mode="no_sf-0"/>
            <xsl:apply-templates select="marc:datafield[@tag > '754' and @tag &lt; '760']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag > '759' and @tag &lt; '788' and count(marc:subfield[@code != 'w']) > 0]" mode="no_sf-w"/>
            <xsl:apply-templates select="marc:datafield[@tag > '787' and @tag &lt; '800']" mode="copying-datafields"/>
            <xsl:apply-templates select="marc:datafield[@tag > '799' and @tag &lt; '831' and count(marc:subfield[@code != '0' and @code != 'w']) > 0]" mode="no_sf-0w"/>
            <xsl:apply-templates select="marc:datafield[(@tag > '830' and @tag &lt; '841') or @tag = '856' or @tag ='880']" mode="copying-datafields"/>
        </record>
    </xsl:template>
    
    <!-- Copying leader - general rules -->
    <xsl:template match="marc:leader">
        <leader><xsl:value-of select="."/></leader>
    </xsl:template>
    
    <!-- Copying controlfields - general rules -->
    <xsl:template match="marc:controlfield">
        <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>        
    </xsl:template>
    
    <!-- Copying datafields - general rules -->
    <xsl:template match="marc:datafield" mode="copying-datafields">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:for-each select="marc:subfield">
                <subfield code="{@code}">
                    <xsl:value-of select="normalize-space(.)"/>
                </subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- Tag 084 -->
    <xsl:template match="marc:datafield[@tag = '084']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:for-each select="marc:subfield">
                <subfield code="{@code}">
                    <xsl:value-of select="."/><xsl:if test="@code = '2' and translate(., 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ') = 'KSSB'"><xsl:text>/8</xsl:text></xsl:if>
                </subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- Tag 508 -->
    <xsl:template match="marc:datafield[@tag = '508']">
        <xsl:param name="leader06"/>
        <xsl:choose>
            <xsl:when test="$leader06 = 'g' or $leader06 = 'i' or $leader06 = 'j' or $leader06 = 'm'">
                <datafield ind1="1" ind2=" " tag="511">
                    <xsl:for-each select="marc:subfield">
                        <subfield code="{@code}">
                            <xsl:value-of select="."/>
                        </subfield>
                    </xsl:for-each>
                </datafield>
            </xsl:when>
            <xsl:otherwise>
                <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                    <xsl:for-each select="marc:subfield">
                        <subfield code="{@code}">
                            <xsl:value-of select="."/>
                        </subfield>
                    </xsl:for-each>
                </datafield>            
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- Tag 100-130, 240, 660-662, 700-754 -->
    <xsl:template match="marc:datafield" mode="no_sf-0">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:for-each select="marc:subfield[@code != '0']">
                <subfield code="{@code}">
                    <xsl:value-of select="."/>
                </subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- Tag 760-787 -->
    <xsl:template match="marc:datafield" mode="no_sf-w">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:for-each select="marc:subfield[@code != 'w']">
                <subfield code="{@code}">
                    <xsl:value-of select="."/>
                </subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- Tag 440, 800-830 -->
    <xsl:template match="marc:datafield[@tag = '440']" mode="to490">
        <datafield ind1="0" ind2=" " tag="490">
            <xsl:if test="marc:subfield[@code = 'a' or @code = 'n' or @code = 'p']">
                <subfield code="a">
                    <xsl:for-each select="marc:subfield[@code = 'a' or @code = 'n' or @code = 'p']">
                        <xsl:value-of select="."/><xsl:text> </xsl:text>
                    </xsl:for-each>
                </subfield>
             </xsl:if>
             <xsl:for-each select="marc:subfield[@code != '0' and @code != 'w' and @code != 'a' and @code != 'n' and @code != 'p']">
                <subfield code="{@code}">
                    <xsl:value-of select="."/>
                </subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <xsl:template match="marc:datafield" mode="no_sf-0w">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:for-each select="marc:subfield[@code != '0' and @code != 'w']">
                <subfield code="{@code}">
                    <xsl:value-of select="."/>
                </subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
</xsl:stylesheet>


