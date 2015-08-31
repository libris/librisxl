package whelk

import groovy.util.logging.Slf4j as Log

import org.picocontainer.*
import org.picocontainer.containers.*
import org.picocontainer.injectors.*

import whelk.component.*

@Log
class PicoWhelk {


    //PicoContainer pico = new DefaultPicoContainer(new CompositeInjection(new ConstructorInjection(), new SetterInjection()))

    void init() {
        log.info("Starting picowhelk.")

        Properties mainProps = new Properties()

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream mainConfig = ( System.getProperty("xl.properties")
                                   ? new FileInputStream(System.getProperty("xl.properties"))
                                   : this.getClass().getClassLoader().getResourceAsStream("whelk.properties") )
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                                      ? new FileInputStream(System.getProperty("xl.secret.properties"))
                                      : this.getClass().getClassLoader().getResourceAsStream("secrets.properties") )

        mainProps.load(mainConfig)
        Properties props = new Properties(mainProps)

        props.load(secretsConfig)
        props.list(System.out)

        MutablePicoContainer pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))

        pico.as(Characteristics.USE_NAMES).addComponent(ElasticSearchClient.class)
        pico.as(Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)

        //pico.addComponent("postgres", PostgreSQLComponent.class, new ConstantParameter("jdbc:postgresql://localhost/whelk"), new ConstantParameter("lddb2"))
        pico.start()
        def postgres = pico.getComponent(PostgreSQLComponent.class)
        log.info("Postgres is ${postgres.getClass().getName()} with connect url: ${postgres.connectionUrl}")
        def doc = postgres.load("/bib/13531679")
        log.info("Loaded doc ${doc.identifier}")
    }
}

