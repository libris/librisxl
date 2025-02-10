'''
Vill vi köra igenom all verk eller ett urval?
Hur hanterar man utbrutna vs ibakade verk?


Räknare:
Antal för varje intersektion -> mappad verkstyp
Antal verk med < 1 instanser
Antal verk med 1 instans
Antal verk med > 1 instans
    Antal verk med > instans och samma issuanceType
    Antal verk med > instans och olika issuanceTypes 
Antal verk med instanser men utan entydig issuanceType
Antal verk som inte kunde sparas


Hämta ett verk
Hämta verkets instsnser
    Om antal instanser == 1:
        Om instans["issuanceType"]:
            Räkna ut nyVerkstyp utifrån verk["@type"] + instans["issuanceType"]
    Om antal instanser > 1:
        Om alla instanser har samma instans["issuanceType"]:
            Ändra verk["@type"] till nyVerkstyp
        Om instanser har olika instance["issuanceType"]:
            Addera till räknare och skriv verks-ID:t till rapport över verk med tvetydig issuanceType. Det har bör vara max en handfull - ge till kataogisatör för att fixa manuellt?
    Om antal instanser < 1:
    Addera till räknare och skriv ut verks-ID:t till rapport över tvetydiga verk. Borde finnas ytterst få - ge till katalogisatör för att fixa manuellt?
Ändra verk["@type"] till nyVerkstyp
Spara post
Om sparandet är framgångsrikt
    Inkrementera räknare för nyVerkstyp
Om sparandet går fel
    Addera till räknare och skriv verks-ID:t till rapport över verk som inte kunde sparas.

'''


someIds = ["j2vzn03v08lfhrm", "q71wpvz219vf4rm", "r934d6m32891v39"]

println("Hello! You fetched some records! Let's have a look...\n")

def records = []

selectByIds(someIds) {
println(it.graph[1]['@type'])
   records.add(it.graph)
   incrementStats('type', it.graph[1]['@type'])
   }

    for (r in records) {

        println("ID: ${r["@id"]}")
        println("Title: ${r["hasTitle"]["mainTitle"]}\n")

    }

println("Good job!")