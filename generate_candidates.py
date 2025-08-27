#!/usr/bin/env python3
import os, sys, time, traceback
import pandas as pd
import requests
from pathlib import Path

# --- nieuw ---
import csv, tempfile, unicodedata, re  # voor veilig CSV schrijven & sanitizing

LIMIT = int(os.environ.get("LIMIT", "200"))
SANITIZE = bool(int(os.environ.get("SANITIZE", "1")))  # 1 = schoonmaken aan (default)

QUERY = """
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
    WHERE {
      BIND(RAND() AS ?sortKey)
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
      FILTER NOT EXISTS {?item wdt:P7763 []}
      FILTER NOT EXISTS {?item wdt:P570 []}
    }
    GROUP BY ?item
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,nl,en". }
}
ORDER BY ?sortKey
LIMIT 10
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

# --- nieuw: sanitizing + veilig (atomic) CSV schrijven ---
COLUMNS = [
    "item","objecttitel","itemLabel","objectsoortLabel",
    "beroepLabel","collectieLabel","floruit","werklocatieLabel","qid"
]

def sanitize_text(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unicodedata.normalize("NFC", s)
    # vervang line/paragraph separators, CR/LF/tab door spaties
    s = s.replace("\u2028", " ").replace("\u2029", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    # niet-printbare control-chars eruit (behalve spatie)
    s = "".join(ch for ch in s if ch.isprintable() or ch == " ")
    # collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    if "item" in df.columns:
        df["qid"] = df["item"].apply(qid_from_uri)
    # zorg dat alle verwachte kolommen er zijn
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""
    # sanitizen tekstvelden
    if SANITIZE:
        for col in ["objecttitel","itemLabel","objectsoortLabel","beroepLabel","collectieLabel","werklocatieLabel"]:
            if col in df.columns:
                df[col] = df[col].map(sanitize_text)
    # volgorde + NaN -> lege string
    df = df[COLUMNS].fillna("")
    return df

def safe_write_csv(df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = prepare_df(df)
    # atomic write: eerst naar tmp, dan replace
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="") as tmp:
        tmp_name = tmp.name
        df.to_csv(
            tmp,
            index=False,
            header=True,
            sep=",",
            quoting=csv.QUOTE_ALL,   # forceer quotes: bestand blijft valide als er komma's/aanhalingstekens/\n in velden zitten
            doublequote=True,
            lineterminator="\n",
        )
    os.replace(tmp_name, out_path)

def main():
    out = Path("data") / "candidates.csv"
    try:
        data = run_query(QUERY)
        df = json_to_df(data)
        safe_write_csv(df, out)
        print(f"[OK] {len(df)} resultaten → {out}")
    except Exception as e:
        print("[ERROR] SPARQL faalde:", e, file=sys.stderr)
        traceback.print_exc()
        # Schrijf lege CSV met juiste kolommen zodat de workflow door kan
        empty = pd.DataFrame(columns=COLUMNS)
        safe_write_csv(empty, out)
        print(f"[WARN] Lege CSV geschreven → {out}")

if __name__ == "__main__":
    main()
    sys.exit(0)
