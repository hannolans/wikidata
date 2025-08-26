#!/usr/bin/env python3
import os, sys, time, traceback, csv, tempfile
from pathlib import Path
import requests
import pandas as pd

# ---- Config ----------------------------------------------------
LIMIT = int(os.environ.get("LIMIT", "10"))
OUTFILE = Path("data") / "candidates.csv"
KEEP_NEWLINES = bool(int(os.environ.get("KEEP_NEWLINES", "0")))  # 0=vervang \n door spatie, 1=laat staan (gequote)
RESPECT_WDQS = float(os.environ.get("WDQS_PAUSE", "0.0"))        # extra pauze na query (s)

UA = os.environ.get(
    "WDQS_USER_AGENT",
    "CopyClear-SPARQL/0.3 (+https://github.com/<user>/<repo>; https://www.wikidata.org/wiki/User:CopyClear)"
)
WDQS_URL = "https://query.wikidata.org/sparql"
HEADERS = {"User-Agent": UA, "Accept": "application/sparql-results+json"}

QUERY_TEMPLATE = """
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
  ?floruit
WHERE {
  {
    SELECT
      ?item
      (SAMPLE(?objectsoort) AS ?objectsoort)
      (SAMPLE(?objecttitel) AS ?objecttitel)
      (SAMPLE(?werkloc0)    AS ?werklocatie)
      (SAMPLE(?beroep0)     AS ?beroep)
      (SAMPLE(?collectie0)  AS ?collectie)
      (SAMPLE(?fy0)         AS ?floruit)
      (SAMPLE(?rand)        AS ?sortKey)
    WHERE {
      BIND(RAND() AS ?rand)
      ?item wdt:P6379 wd:Q1616123 ;
            wdt:P31  wd:Q5 .

      OPTIONAL { ?item wdt:P1317 ?floruit1 . BIND(YEAR(?floruit1) AS ?fy0) }
      OPTIONAL { ?item wdt:P937  ?werkloc0. }
      OPTIONAL { ?item wdt:P106  ?beroep0. }
      OPTIONAL { ?item wdt:P6379 ?collectie0. }

      ?item p:P6379 [
        ps:P6379 wd:Q1616123 ;
        prov:wasDerivedFrom [
          pr:P3865 ?objectsoort ;
          pr:P1476 ?objecttitel
        ]
      ] .
      FILTER NOT EXISTS { ?item wdt:P7763 [] }
    }
    GROUP BY ?item
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,nl,en". }
}
ORDER BY ?sortKey
LIMIT {limit}
"""

COLUMNS = [
    "item", "objecttitel", "itemLabel", "objectsoortLabel",
    "beroepLabel", "collectieLabel", "floruit", "werklocatieLabel", "qid"
]

# ---- Helpers ---------------------------------------------------
def run_query(query: str, tries=5, backoff=2.0):
    for i in range(tries):
        r = requests.get(WDQS_URL, params={"query": query}, headers=HEADERS, timeout=60)
        if r.status_code == 200:
            return r.json()
        print(f"[WARN] WDQS HTTP {r.status_code} (attempt {i+1}/{tries})", file=sys.stderr)
        try: print(r.text[:1000], file=sys.stderr)
        except Exception: pass
        time.sleep(backoff * (i+1))
    raise RuntimeError(f"WDQS failed after {tries} attempts")

def json_to_df(data: dict) -> pd.DataFrame:
    rows = []
    for b in data.get("results", {}).get("bindings", []):
        row = {k: v.get("value") for k, v in b.items()}
        rows.append(row)
    return pd.DataFrame(rows)

def qid_from_uri(u: str) -> str:
    return u.rsplit('/', 1)[-1] if isinstance(u, str) and u.startswith('http') else u

def sanitize_text(s: str) -> str:
    if not isinstance(s, str):
        return s
    # verwijder carriage returns altijd; newlines eventueel behouden
    s = s.replace("\r", " ")
    if KEEP_NEWLINES:
        return s
    # normaliseer alle whitespace (incl. \n, \t) naar spaties
    return " ".join(s.split())

def write_atomic_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="") as tmp:
        tmp_name = tmp.name
        df.to_csv(
            tmp,
            index=False,
            header=True,
            sep=",",
            quoting=csv.QUOTE_ALL,      # forceer quotes -> geen 'malformed' door embedded \n/,
            doublequote=True,
            lineterminator="\n",
        )
    os.replace(tmp_name, path)

# ---- Main ------------------------------------------------------
def main():
    try:
        query = QUERY_TEMPLATE.format(limit=LIMIT)
        data = run_query(query)
        if RESPECT_WDQS > 0:
            time.sleep(RESPECT_WDQS)

        df = json_to_df(data)

        if df.empty:
            df = pd.DataFrame(columns=COLUMNS)
        else:
            # Afgeleide kolommen, schoonmaak en vaste volgorde
            if "item" in df.columns:
                df["qid"] = df["item"].apply(qid_from_uri)

            for col in ["objecttitel", "itemLabel", "objectsoortLabel", "beroepLabel",
                        "collectieLabel", "werklocatieLabel"]:
                if col in df.columns:
                    df[col] = df[col].map(sanitize_text)

            # Zorg dat alle verwachte kolommen bestaan
            for col in COLUMNS:
                if col not in df.columns:
                    df[col] = ""

            # Alleen de gewenste kolommen en in vaste volgorde
            df = df[COLUMNS]

            # NaN -> lege string (anders wordt 'nan' weggeschreven)
            df = df.fillna("")

        write_atomic_csv(df, OUTFILE)
        print(f"[OK] {len(df)} resultaten → {OUTFILE}")
    except Exception as e:
        print("[ERROR] SPARQL faalde:", e, file=sys.stderr)
        traceback.print_exc()
        # Schrijf lege CSV met juiste kolommen zodat de workflow door kan
        pd.DataFrame(columns=COLUMNS).to_csv(
            OUTFILE, index=False, encoding="utf-8", lineterminator="\n", quoting=csv.QUOTE_ALL
        )
        print(f"[WARN] Lege CSV geschreven → {OUTFILE}")

if __name__ == "__main__":
    main()
    sys.exit(0)
