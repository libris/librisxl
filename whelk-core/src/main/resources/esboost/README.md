# Boostning i Elasticsearch

Detta dokument beskriver inställningarna i konfigurationsfilen `settings.json` som styr hur sökresultat rankas i
Elasticsearch för att ge mer relevanta träffar.
Inställningarna är uppdelade i tre huvuddelar: **field_boost**, **function_score** och **constant_score**.

---

## `field_boost`

Anger vilka fält att fritextsöka i samt hur relevanspoängen ska beräknas för dessa fält.

| Parameter                          | Beskrivning                                                                                                                                                                                           |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fields`                           | Lista med fält som ska boostas.                                                                                                                                                                       |
| &emsp;&emsp;`name`                 | Fältets namn.                                                                                                                                                                                         |
| &emsp;&emsp;`boost`                | Boostvärde.                                                                                                                                                                                           |
| &emsp;&emsp;`script_score`         | [Skriptfunktion](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-script-score-query) som modifierar poängen.                                                                |
| &emsp;&emsp;&emsp;&emsp;`name`     | Beskrivande namn på funktionen.                                                                                                                                                                       |
| &emsp;&emsp;&emsp;&emsp;`function` | Funktionen som modifierar poängen.                                                                                                                                                                    |
| &emsp;&emsp;&emsp;&emsp;`apply_if` | Villkor för när funktionen ska appliceras.                                                                                                                                                            |
| `default_boost_factor`             | Boostvärde för fält som ej angetts i `fields`.                                                                                                                                                        |
| `phrase_boost_divisor`             | Styr hur mycket extra poäng som ges till dokument som matchar exakt ordföljd i söksträngar med flera ord. Ju lägre värde desto mer premieras matchning på exakt fras i relevansrankningen.            |
| `analyze_wildcard`                 | Elastic-parameter, se förklaring i [dokumentationen](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-simple-query-string-query#analyze_wildcard).                           |
| `multi_match_type`                 | Anger explicit hur poängen ska räknas ihop då flera fält matchar, se [möjliga värden](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-multi-match-query#multi-match-types). |
| `include_exact_fields`             | Boosta .exact-fält motsvarande respektive fält som angetts i `fields`.                                                                                                                                |

---

## `function_score`

Modifierar relevanspoängen för dokumenten i träffmängden baserat på t.ex. olika fältvärden. Se Elastic-dokumentation.

| Parameter            | Beskrivning                                                                                                                                                                                             |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `functions`          | Lista över [funktioner](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-function-score-query#score-functions) som modifierar poängen.                                         |
| &emsp;&emsp;`type`   | Typ av funktion, t.ex. [field_value_factor](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-function-score-query#function-field-value-factor).                                |
| &emsp;&emsp;`params` | Parametrar för funktionen.                                                                                                                                                                              |
| &emsp;&emsp;`weight` | Faktor som funktionsvärdet multipliceras med.                                                                                                                                                           |
| `score_mode`         | Elastic-parameter som styr hur poängen från flera funktioner kombineras. Se [dokumentation](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-function-score-query).            |
| `boost_mode`         | Elastic-parameter som styr hur poängen från `function_score` kombineras med resten. Se [dokumentation](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-function-score-query). |

---

## `constant_score`

Lista med specifika fältvärden som ger extra poäng till dokument i träffmängden som matchar.

| Parameter | Beskrivning                   |
|-----------|-------------------------------|
| `field`   | Fält som ska matchas.         |
| `value`   | Värde som ger konstant poäng. |
| `score`   | Poängen som läggs till.       |