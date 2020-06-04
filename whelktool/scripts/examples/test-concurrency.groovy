import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

def data =
        [ "@graph": [
                [
                        "@id": "TEMPID",
                        "mainEntity" : ["@id": "TEMPID#it"]
                ],
                [
                        "@id": "TEMPID#it",
                        "@type": "Instance",
                        counter: 0
                ]
        ]]

// Create doc
def item = create(data)
selectFromIterable([item], { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave()
})

def uri = item.doc.getURI().toString()
def update = {
    selectByIds([uri], { i ->
        i.graph[1]['counter'] += 1
        i.scheduleSave()
    })
}

// Simulate parallel scripts
int n = 200
def threadPool = Executors.newFixedThreadPool(20)
try {
    n.times {
        threadPool.submit(update as Runnable);
    }
}
finally {
    threadPool.shutdown()
    threadPool.awaitTermination(5, TimeUnit.MINUTES)
}

// Verify and clean up
selectByIds([uri], { i ->
    int value = i.graph[1]['counter']
    println("VALUE: ${value}")
    i.scheduleDelete()
    assert value == n
})
