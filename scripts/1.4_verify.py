import json
from collections import Counter
from pathlib import Path

import networkx as nx

OUT_CSV_DIR = Path("data/processed/kg/csv")
OUT_GRAPH_DIR = Path("data/processed/kg/graph")
GRAPH_PATH = OUT_GRAPH_DIR / "verdantis_kg.graphml"
META_PATH = OUT_GRAPH_DIR / "metadata.json"

TABLES = [
    "organization",
    "asset",
    "permit",
    "document",
    "policy_clause",
    "satellite_tile",
    "iot_stream",
    "event",
    "rule",
    "proof_bundle",
]


def main() -> None:
    ok = True

    # CSV presence
    for t in TABLES:
        p = OUT_CSV_DIR / f"{t}.csv"
        exists = p.exists()
        print(f"[csv] {t}: {'OK' if exists else 'MISSING'} ({p})")
        ok &= exists

    # Graph
    if GRAPH_PATH.exists():
        G = nx.read_graphml(GRAPH_PATH)
        n, m = G.number_of_nodes(), G.number_of_edges()
        print(f"[graph] nodes={n}, edges={m}")
        ent = Counter(nx.get_node_attributes(G, "entity").values())
        print("[graph] nodes by entity:", dict(ent))
    else:
        print(f"[graph] MISSING: {GRAPH_PATH}")
        ok = False

    # Metadata
    if META_PATH.exists():
        meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        print(f"[meta] {META_PATH} → nodes={meta.get('nodes')}, " f"edges={meta.get('edges')}")
    else:
        print(f"[meta] MISSING: {META_PATH}")
        ok = False

    if not ok:
        raise SystemExit("M1.4 verification FAILED.")
    print("M1.4 verification passed.")


if __name__ == "__main__":
    main()
