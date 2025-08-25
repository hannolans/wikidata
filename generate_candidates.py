#!/usr/bin/env python3
import os, sys, time, traceback
import pandas as pd
import requests
from pathlib import Path

LIMIT = int(os.environ.get("LIMIT", "200"))

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
  (SAMPLE(?fy) AS ?floruit)
WHERE {{
  {{
    SELECT
      ?item
      (SAMPLE(?objectsoort) AS ?objectsoort)
      (SAMPLE(?objecttitel) AS ?objecttitel)
      (SAMPLE(?werkloc0)    AS ?werklocatie)
      (SAMPLE(?beroep0)     AS ?beroep)
      (SAMPLE(?collectie0)  AS ?collectie)
      (SAMPLE(?fy0)         AS ?fy)
    WHERE {{
      ?item wdt:P6379 wd:Q1616123 ;
            wdt:P31  wd:Q5 .
      OPTIONAL {{ ?item wdt:P1317 ?floruit1 . BIND(YEAR(?floruit1) AS ?fy0) }}
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

UA = os.environ.get(
    "WDQS_USER_AGENT",
    "CopyClear-SPARQL/0.2 (+https://github.com/<user>/<repo>; https://www.wikidata.org/wiki/User:CopyClear)"
)
WDQS_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/sparql-results+json"
}

def run_query(query: str, tries=5, backoff=2.0):
    for i in range(tries):
        r = requests.get(WDQS_URL, params={"query": query}, headers=HEADERS, timeout=60)
        if r.status_code == 200:
            return r.json()
        # WDQS geeft soms 400 bij throttling; body bevat hint
        print(f"[WARN] WDQS HTTP {r.status_code} (attempt {i+1}/{tries})", file=sys.stderr)
        try:
            print(r.text[:1000], file=sys.stderr)
        except Exception:
            pass
        time.sleep(backoff * (i+1))
    raise RuntimeError(f"WDQS failed after {tries} attempts")

def json_to_df(data: dict) -> pd.DataFrame:
    rows = []
    for b in data.get("results", {}).get("bindings", []):
        row = {}
        for k, v in b.items():
            row[k] = v.get("value")
        rows.append(row)
    return pd.DataFrame(rows)

def qid_from_uri(u: str) -> str:
    return u.rsplit('/', 1)[-1] if isinstance(u, str) and u.startswith('http') else u

def main():
    out = Path("data") / "candidates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        data = run_query(QUERY)
        df = json_to_df(data)
        if not df.empty and "item" in df.columns:
            df["qid"] = df["item"].apply(qid_from_uri)
        df.to_csv(out, index=False, encoding="utf-8")
        print(f"[OK] {len(df)} resultaten → {out}")
    except Exception as e:
        print("[ERROR] SPARQL faalde:", e, file=sys.stderr)
        traceback.print_exc()
        # Schrijf lege CSV met juiste kolommen zodat de workflow door kan
        pd.DataFrame(columns=[
            "item","itemLabel","objectsoortLabel","objecttitel",
            "werklocatieLabel","beroepLabel","collectieLabel","floruit","qid"
        ]).to_csv(out, index=False, encoding="utf-8")
        print(f"[WARN] Lege CSV geschreven → {out}")

if __name__ == "__main__":
    main()
    sys.exit(0)
