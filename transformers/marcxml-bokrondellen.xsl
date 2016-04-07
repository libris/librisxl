<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:marc="http://www.loc.gov/MARC21/slim"
    exclude-result-prefixes="marc">
    <xsl:output indent="yes"/>
    
    <xsl:template match="/">
        <xsl:apply-templates/>
    </xsl:template>
    
    <xsl:template match="marc:record">
        <xsl:variable name="encoding_level"><xsl:value-of select="substring(marc:leader, 18, 1)"/></xsl:variable>
        
        <xsl:if test="(count(marc:datafield[@tag = '020']/marc:subfield[@code = 'a']) = 1 or count(marc:datafield[@tag = '022']/marc:subfield[@code = 'a']) = 1) and $encoding_level != 5 and $encoding_level != 8">
            <artikel>
                <xsl:call-template name="artikelnummer"/>
                <xsl:apply-templates select="marc:datafield[@tag = '020']/marc:subfield[@code = 'a']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '022']/marc:subfield[@code = 'a']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '084']/marc:subfield[@code = 'a']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '650']/marc:subfield[@code = 'a']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '245']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '100']/marc:subfield[@code = 'a']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '700']/marc:subfield[@code = 'a']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '260']/marc:subfield[@code = 'b']"/>
                <xsl:apply-templates select="marc:datafield[@tag = '440']"/>
                <xsl:apply-templates select="marc:controlfield[@tag = '008']"/>
                <xsl:apply-templates select="marc:leader"/>
            </artikel>
        </xsl:if>
    </xsl:template>

    <xsl:template name="artikelnummer">
      <xsl:variable name="isbn"><xsl:value-of select="marc:datafield[@tag = '020']/marc:subfield[@code = 'a']"/></xsl:variable>
      <xsl:variable name="issn"><xsl:value-of select="marc:datafield[@tag = '022']/marc:subfield[@code = 'a']"/></xsl:variable>
      <xsl:variable name="artikelnummer">
        <xsl:choose>
          <xsl:when test="substring-before($isbn, '(') != ''"><xsl:value-of select="normalize-space(substring-before(translate($isbn, '-', ''), '('))"/></xsl:when>
          <xsl:when test="substring-before($isbn, ' ') != ''"><xsl:value-of select="normalize-space(substring-before(translate($isbn, '-', ''), ' '))"/></xsl:when>
          <xsl:when test="$isbn != ''"><xsl:value-of select="translate($isbn, '-', '')"/></xsl:when>
          <xsl:when test="$issn != ''"><xsl:value-of select="translate($issn, '-', '')"/></xsl:when>
        </xsl:choose>
      </xsl:variable>
      <!--<xsl:comment>isbn: '<xsl:value-of select="$isbn"/>', n_isbn: '<xsl:value-of select="count(marc:datafield[@tag = '020']/marc:subfield[@code = 'a'])"/>', issn: '<xsl:value-of select="$issn"/>', n_issn: '<xsl:value-of select="count(marc:datafield[@tag = '022']/marc:subfield[@code = 'a'])"/>', biblevel: '<xsl:value-of select="substring(marc:leader, 18, 1)"/>', artikelnummer: <xsl:value-of select="normalize-space($artikelnummer)"/></xsl:comment>-->
      <artikelnummer><xsl:value-of select="normalize-space($artikelnummer)"/></artikelnummer>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '020']/marc:subfield[@code = 'a']">
       <xsl:choose>
            <xsl:when test="substring-before(., '(') != ''">
                <isbn><xsl:value-of select="normalize-space(substring-before(., '('))"/></isbn>
                <bandtyp><xsl:value-of select="normalize-space(translate(substring-after(., '('), '():;', ''))"/></bandtyp>
            </xsl:when>
            <xsl:when test="substring-before(., ' ') != ''">
                <isbn><xsl:value-of select="normalize-space(substring-before(., ' '))"/></isbn>
                <bandtyp><xsl:value-of select="normalize-space(translate(substring-after(., ' '), '():;', ''))"/></bandtyp>
            </xsl:when>
            <xsl:otherwise>
                <isbn><xsl:value-of select="."/></isbn>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '084']/marc:subfield[@code = 'a']">
        <katalogsignum><xsl:value-of select="."/></katalogsignum>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '650']/marc:subfield[@code = 'a']">
        <amnesord><xsl:value-of select="."/></amnesord>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '245']">
        <xsl:variable name="titel"><xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b' or @code = 'n' or @code = 'p']"><xsl:value-of select="concat(., ' ')"/></xsl:for-each></xsl:variable>
        <xsl:choose>
            <xsl:when test="substring($titel, string-length($titel)-1, 1) = '/'">
                <titel><xsl:value-of select="normalize-space(substring($titel, 1, string-length($titel)-3))"/></titel>
            </xsl:when>
            <xsl:otherwise>
                <titel><xsl:value-of select="normalize-space($titel)"/></titel>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '100']/marc:subfield[@code = 'a']">
        <xsl:variable name="forfattare"><xsl:value-of select="."/></xsl:variable>
        <!--<test3><xsl:value-of select="substring($forfattare, string-length($forfattare), 1)"/></test3>-->
        <!--<test4><xsl:value-of select="$forfattare"/></test4>-->
        <xsl:choose>
            <xsl:when test="substring($forfattare, string-length($forfattare), 1) = ','">
                <medarbetare type="forfattare"><xsl:value-of select="normalize-space(substring($forfattare, 1, string-length($forfattare)-1))"/></medarbetare>
            </xsl:when>
            <xsl:otherwise>
                <medarbetare type="forfattare"><xsl:value-of select="$forfattare"/></medarbetare>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!--
    <xsl:template match="datafield[@tag = '700']/subfield[@code = 'a']">
        <medarbetare><xsl:value-of select="."/></medarbetare>
    </xsl:template>
    -->

    <xsl:template match="marc:datafield[@tag = '260']/marc:subfield[@code = 'b']">
        <forlag><xsl:value-of select="translate(., ',', '')"/></forlag>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag = '440']">
        <xsl:variable name="serie"><xsl:for-each select="subfield[@code != '9']"><xsl:value-of select="concat(., ' ')"/></xsl:for-each></xsl:variable>
        <serie><xsl:value-of select="normalize-space(.)"/></serie>
    </xsl:template>

    <xsl:template match="marc:controlfield[@tag = '008']">
        <utgivningsdatum><xsl:value-of select="substring(., 8, 4)"/></utgivningsdatum>
    </xsl:template>

    <xsl:template match="marc:leader">
        <xsl:choose>
            <xsl:when test="substring(., 7, 1) = 'a'">
                <mediatyp>bok</mediatyp>
            </xsl:when>
            <xsl:when test="substring(., 7, 1) = 'i'">
                <mediatyp>ljudbok</mediatyp>
            </xsl:when>
            <xsl:when test="substring(., 7, 1) = 'm'">
                <mediatyp>multimedium</mediatyp>
            </xsl:when>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="text()"/>
</xsl:stylesheet>
