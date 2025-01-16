someIds = ["j2vzn03v08lfhrm", "q71wpvz219vf4rm", "r934d6m32891v39"]

println("Hello! You fetched some records! Let's have a look...\n")

def records = []

selectByIds(someIds) {
   records.add(it.graph)
   incrementStats('type', it.graph[1]['@type'])
   }

    for (r in records) {

        println("ID: ${r["@id"]}")
        println("Title: ${r["hasTitle"]["mainTitle"]}\n")

    }

println("Good job!")