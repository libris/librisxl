#LibrisIntegration

Den här Apache Camel-baserade Spring-webb-applikationen är tänkt att hantera integrationsflöden mellan olika Libris-system. Till exempel: 

* mellan LibrisXL och APIX (för skrivning av uppdateringar som gjorts i LibrisXL till Voyager)
* mellan LibrisXL och Elasticsearch för asynkron indexering

JMS-baserade Apache ActiveMQ används som meddelande-server och broker.

En lokal Elasticsearch-instans skapas vid körning. Gå till [http://localhost:9200/] (http://localhost:9200/) för att spana in den.

Än så länge är applikationen ett skelett, mer följer...

Läs mer om [Apache Camel] (http://camel.apache.org/)

Läs mer om [Apache Camel och Spring Framework] (http://camel.apache.org/spring.html)

Läs mer om [Apache ActiveMQ] (http://activemq.apache.org/)

Läs mer om [JMS] (https://docs.oracle.com/javaee/7/api/javax/jms/package-summary.html#package.description)

Läs mer om [Elasticsearch] (https://www.elastic.co/products/elasticsearch)


### Teknik och versioner

Java 1.7

Apache Camel 2.15.3 (integrationsramverk)

Spring web framework 4.2.1 (webbramverk)

ActiveMQ 5.8.0 (meddelande-server och broker)

Gradle 2.4 (bygg och dependency-hantering)


### Installera ActiveMQ

brew install activemq

alternativt ladda ner och installera via [http://activemq.apache.org/] (http://activemq.apache.org/)


### Starta ActiveMQ

activemq start


### Kolla in ActiveMQ-köer

http://127.0.0.1:8161/admin/ (admin/admin)


### Sätta upp projektet i IntelliJ IDEA

Checka ut projektet från github, till exempel via [GitHub Desktop] (https://desktop.github.com/).
I IntelliJ IDEA: File/New/Project from Existing Sources


### Starta som separat applikation (för test)

Använd IntelliJ IDEAs Gradle-plugin (gå till File/Other Settings/Configure Plugins för att se till att den är installerad). Kör "installApp" och "run" från Gradle



