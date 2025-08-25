#!/usr/bin/env python3
import os
import pandas as pd
from pathlib import Path
from pywikibot.data.sparql import SparqlQuery

LIMIT = int(os.environ.get("LIMIT", "200"))  # stel via env var in de workflow

QUERY = f"""
PREFIX wd:   <http://www.wikidata.org/entity/>
PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX p:    <http://www.wikidata.org/prop/>
PREFIX ps:   <http://www.wikidata.org/prop/statement/>
PREFIX pr:   <http://www.wikidata.org/prop/reference/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX bd:   <http://www.bigdata.com/rdf#>

SELECT
  ?item ?itemLabel
  ?objectsoortLabel
  ?objecttitel
  ?werklocatieLabel
  ?beroepLabel
  ?collectieLabel
  (SAMPLE(?floruitYear1) AS ?floruit)
WHERE {{
  {{
    SELECT
      ?item
      (SAMPLE(?objectsoort) AS ?objectsoort)
      (SAMPLE(?objecttitel) AS ?objecttitel)
      (SAMPLE(?werkloc0)    AS ?werklocatie)
      (SAMPLE(?beroep0)     AS ?beroep)
      (SAMPLE(?collectie0)  AS ?collectie)
      (SAMPLE(?fy)          AS ?floruitYear1)
    WHERE {{
      ?item wdt:P6379 wd:Q1616123 ;   # in collectie NADD
            wdt:P31  wd:Q5 .          # mens

      OPTIONAL {{ ?item wdt:P1317 ?floruit1 . BIND(YEAR(?floruit1) AS ?fy) }}
      OPTIONAL {{ ?item wdt:P937  ?werkloc0. }}
      OPTIONAL {{ ?item wdt:P106  ?beroep0. }}
      OPTIONAL {{ ?item wdt:P6379 ?collectie0. }}
      FILTER NOT EXISTS {{ ?item wdt:P7763 [] . }}

      ?item p:P6379 [
        ps:P6379 wd:Q1616123 ;
        prov:wasDerivedFrom [
          pr:P3865 ?objectsoort ;
          pr:P1476 ?objecttitel
        ]
      ] .
    }}
    GROUP BY ?item
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,nl,en". }}
}}
LIMIT {LIMIT}
"""

def qid_from_uri(u: str) -> str:
    return u.rsplit('/', 1)[-1] if isinstance(u, str) and u.startswith('http') else u

def main():
    # Nette User-Agent voor pywikibot SPARQL
    os.environ.setdefault(
        "PYWIKIBOT2_NO_USER_CONFIG", "1"
    )  # geen lokale user-config nodig in Actions
    os.environ.setdefault(
        "PYWIKIBOT_API_USERAGENT",
        "CopyClear-SPARQL/0.1 (GitHub Actions; https://github.com/<your-user>/<your-repo>)"
    )

    sq = SparqlQuery()
    rows = sq.select(QUERY)  # List[dict]
    for r in rows:
        if 'item' in r:
            r['qid'] = qid_from_uri(r['item'])

    df = pd.DataFrame(rows)
    out = Path("data") / "candidates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"[OK] {len(df)} resultaten → {out}")

if __name__ == "__main__":
    main()
