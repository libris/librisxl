#LibrisIntegration

Den här Apache Camel-baserade Spring-webb-applikationen är tänkt att hantera integrationsflöden mellan olika Libris-system. Till exempel: 

* mellan LibrisXL och APIX (för skrivning av uppdateringar som gjorts i LibrisXL till Voyager)
* mellan LibrisXL och Elasticsearch för asynkron indexering

JMS-baserade Apache ActiveMQ används som meddelande-server och broker med köhantering.

Läs mer om [Apache Camel] (http://camel.apache.org/)

Läs mer om [Apache Camel och Spring Framework] (http://camel.apache.org/spring.html)

Läs mer om [Apache ActiveMQ] (http://activemq.apache.org/)

Läs mer om [JMS] (https://docs.oracle.com/javaee/7/api/javax/jms/package-summary.html#package.description)


### Teknik och versioner

Java 1.7

Apache Camel 2.15.3 (integrationsramverk)

Spring web framework 4.2.1 (webbramverk)

ActiveMQ 5.8.0 (meddelande-server och broker med köhantering)

Gradle 2.4 (bygg och dependency-hantering)


### Sätta upp projektet i IntelliJ IDEA

Checka ut projektet från github, till exempel via [GitHub Desktop] (https://desktop.github.com/).
I IntelliJ IDEA: File/New/Project from Existing Sources

Fixa properties-fil för integrations-modulen. Från librisxl-katalogen:
$ cd integration/src/main/resources
$ cp integration.properties.in integration.properties
kontrollera inställningarna i properties-filen


### Testa APIX-integration med lokala installationer av ActiveMQ, PostgreSQL, Elasticsearch och en mockad APIX-server

Installera och starta PostgreSQL:

$ brew install postgresql
alternativt ladda ner och installera via [http://www.postgresql.org/download/] (http://www.postgresql.org/download/)
$ postgres -D /usr/local/var/postgres


Installera och starta Elasticsearch:

$ brew install elasticsearch
alternativt ladda ner och installera via [https://www.elastic.co/downloads/elasticsearch] (https://www.elastic.co/downloads/elasticsearch)
$ elasticsearch (kolla in via localhost:9200)


Installera och starta activemq:

$ brew install activemq 
alternativt ladda ner och installera via [http://activemq.apache.org/] (http://activemq.apache.org/)

$ activemq start (se http://127.0.0.1:8161/admin (admin/admin))


Starta mock APIX-server. Från librisxl-katalogen: 

$ cd librisxl-tools/scripts
$ python mock_apix_server.py (körs på localhost:8100)


Starta integrations-modulen. Från librisxl-katalogen:

$ cd integration
$ gradle jettyrun <port>
alternativt konfigurera gradle i IntelliJ IDEA genom att först se till att gradle-plugin är installerad (File/Other settings/Configure Plugins), sen konfigurera gradle under Preferences/Build, execution, deployment/Build tools/gradle och via Run/Edit configurations/+/Gradle


Starta Whelk-REST-API. Från librisxl-katalogen:

$ cd rest
$ gradle jettyrun <port>


Posta ett dokument:

$ curl -XPOST -H "content-type:application/ld+json" -d @dok.json

När ett dokument postas via REST-api:et skapas ett meddelande (se whelk.component.ApixClientCamel i core-modulen) som läggs på ActiveMQ-kö (konfigurerad som activemq_apix_queue i integration.properties). Integrationsmodulen läser meddelanden från kön och skriver till APIX. 


### Enhetstester
Enhetstester under /src/test/groovy.

Kör testerna:
$ gradle test


### Starta som separat applikation (för test)

Använd IntelliJ IDEAs Gradle-plugin (gå till File/Other Settings/Configure Plugins för att se till att den är installerad). Kör "installApp" och "run" från Gradle


