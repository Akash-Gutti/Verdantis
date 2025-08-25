"""Streamlit UI — Verify Proof (M9.4).

Run:
  streamlit run apps/m9_verify_app.py --server.port 8503
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import httpx
import streamlit as st


def _default_base() -> str:
    return os.environ.get("ZK_SVC_URL", "http://127.0.0.1:8011").rstrip("/")


def _post_verify(base_url: str, bundle_obj: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{base_url}/verify"
    with httpx.Client(timeout=10.0) as client:
        r = client.post(url, json={"bundle": bundle_obj})
        r.raise_for_status()
        return r.json()


st.set_page_config(page_title="Verdantis — Verify Proof", layout="centered")
st.title("✅ Verify Proof (M9.4)")

base_url = st.text_input(
    "zk-svc base URL", value=_default_base(), help="Your running zk-svc /verify endpoint"
)

tab1, tab2 = st.tabs(["Paste JSON", "Upload file"])

bundle_data: Optional[Dict[str, Any]] = None

with tab1:
    txt = st.text_area("Paste bundle JSON here", height=220)
    if st.button("Use pasted JSON", type="secondary"):
        try:
            obj = json.loads(txt)
            if not isinstance(obj, dict):
                st.error("Pasted JSON must be an object (a bundle).")
            else:
                bundle_data = obj
                st.success("Bundle loaded from pasted JSON.")
        except json.JSONDecodeError as exc:
            st.error(f"JSON parse error: {exc}")

with tab2:
    up = st.file_uploader("Upload bundle JSON file", type=["json"])
    if up is not None:
        try:
            raw = up.read().decode("utf-8")
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                st.error("File must contain a JSON object (a bundle).")
            else:
                bundle_data = obj
                st.success(f"Loaded bundle from file: {up.name}")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to read file: {exc}")

st.divider()
if st.button("Verify Proof", type="primary") and bundle_data is not None:
    try:
        resp = _post_verify(base_url, bundle_data)
        if bool(resp.get("ok")) and bool(resp.get("valid")):
            st.success("Proof verified ✅")
        else:
            st.error(f"Proof invalid ❌ — reasons: {resp.get('reasons')}")
        # Show a compact summary card
        b = bundle_data
        with st.expander("Bundle summary"):
            st.json(
                {
                    "bundle_id": b.get("bundle_id"),
                    "decision": b.get("decision"),
                    "model_id": b.get("model_id"),
                    "model_version": b.get("model_version"),
                    "score": b.get("score"),
                    "threshold": b.get("threshold"),
                    "pdf_hash_prefix": (b.get("pdf_hash") or "")[:16],
                    "feature_commit_prefix": (b.get("feature_commit") or "")[:16],
                    "signer": b.get("signer"),
                }
            )
    except httpx.HTTPError as exc:
        st.error(f"Request failed: {exc}")
elif st.button("Verify Proof") and bundle_data is None:
    st.warning("Please paste JSON or upload a file first.")
