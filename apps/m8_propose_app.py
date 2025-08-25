"""Streamlit UI for M8.3 — propose rules from text.

Run:
  streamlit run apps/m8_propose_app.py --server.port 8502
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from modules.m8.m8_3_propose import PROPOSED_DIR, propose_from_text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


st.set_page_config(page_title="Verdantis — Propose Rules", layout="centered")
st.title("🧩 Verdantis — Propose Rules (M8.3)")

with st.form("propose"):
    text = st.text_area("Policy clause / description", height=180)
    owner = st.text_input("Owner", value="policy-team")
    severity = st.selectbox("Severity (suggestion)", ["", "low", "medium", "high", "critical"])
    id_hint = st.text_input("ID hint (optional)")
    save = st.checkbox("Save to data/rules/proposed/", value=True)
    submitted = st.form_submit_button("Propose")

if submitted:
    sev = severity if severity else None
    try:
        pairs = propose_from_text(text=text, owner=owner, severity=sev, id_hint=id_hint, save=save)
    except ValueError as exc:
        st.error(f"Validation error: {exc}")
    else:
        st.success(f"Generated {len(pairs)} candidate(s). Folder: {PROPOSED_DIR}")
        for idx, (yaml_text, rule) in enumerate(pairs, start=1):
            st.subheader(f"Candidate #{idx} — {rule['meta']['id']}")
            st.code(yaml_text, language="yaml")
