from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

from modules.m5.m5_2_data import fetch_doc_citations  # NEW
from modules.m5.m5_2_data import fetch_doc_proof_bundle  # NEW
from modules.m5.m5_2_data import fetch_evidence_windowed  # ← use this
from modules.m5.m5_2_data import fetch_kg_edges  # NEW
from modules.m5.m5_2_data import (
    fetch_asset_list,
    fetch_asset_meta,
    fetch_assets_geojson,
    fetch_overlays_geojson,
)

feed_path = Path("data/processed/m10/ui/alerts_feed.json")
if feed_path.exists():
    items = json.loads(feed_path.read_text(encoding="utf-8"))


# --- helpers ---------------------------------------------------------------
def hard_refresh() -> None:
    st.cache_data.clear()
    if hasattr(st, "rerun"):
        st.rerun()


@st.cache_data(show_spinner=False, ttl=30)
def _assets_fc(limit: int) -> Dict[str, Any]:
    return fetch_assets_geojson(limit=limit)


@st.cache_data(show_spinner=False, ttl=30)
def _overlays_fc(limit: int) -> Dict[str, Any]:
    return fetch_overlays_geojson(limit=limit)


@st.cache_data(show_spinner=False, ttl=30)
def _asset_list(limit: int) -> List[tuple[str, str]]:
    return fetch_asset_list(limit=limit)


def _role_colors(role: str) -> Dict[str, str]:
    if role == "regulator":
        return {"asset": "#0041a8", "overlay": "#b30000"}  # blue, red
    if role == "investor":
        return {"asset": "#0e7c7b", "overlay": "#6c757d"}  # teal, gray
    return {"asset": "#2a9d8f", "overlay": "#aaaaaa"}  # public


def _risk_score(published_at: Optional[str], citations: int) -> float:
    """
    Simple investor score: citations weight + freshness bonus.
    """
    c = max(int(citations or 0), 0)
    fresh = 0.0
    if published_at:
        try:
            dt = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
        except Exception:
            dt = None
        if dt:
            days = max((datetime.now(timezone.utc) - dt).days, 0)
            fresh = max(0.0, (365.0 - float(days)) / 365.0)  # 0..1
    return (3.0 * float(c)) + (2.0 * fresh)


# --------------- page --------------------

st.set_page_config(
    page_title="Verdantis | Digital Twin (M5.4)",
    layout="wide",
    page_icon="🌍",
)

st.title("🌍 verdantis — Geospatial Digital Twin (M5.4)")
st.caption(
    "Map + overlays. Evidence panel shows docs, citations, KG edges, proof bundle id. "
    "Role theming affects filters, sorting, and styling."
)

# Sidebar
with st.sidebar:
    st.header("Controls")
    role = st.radio("Role", ["regulator", "investor", "public"], index=0)

    years = st.slider("Event window (years)", 1, 5, 1, step=1)
    days = int(years) * 365

    # Enforce gate for regulator regardless of checkbox
    require_gate_opt_in = st.checkbox(
        "Require ≥2 citations (gate view)", value=(role == "regulator")
    )

    limit = st.slider("Max features", 50, 1000, 300, step=50)

    # Role-driven overlay visibility: public hides overlays regardless
    show_overlay_opt = st.checkbox("Show proximity overlay (250m)", value=True)
    show_assets = st.checkbox("Show assets", value=True)

    if st.button("Refresh data"):
        hard_refresh()

    st.markdown("---")
    st.subheader("Select Asset (fallback)")
    options = _asset_list(limit=1000)
    id_by_name = {n: a for (a, n) in options}
    sel_name = st.selectbox(
        "If map click is not captured, choose here",
        options=[n for (_, n) in options] if options else [],
        index=0 if options else None,
    )
    selected_asset: Optional[str] = id_by_name.get(sel_name) if sel_name else None

# Policy toggles resolved after role:
require_gate = (role == "regulator") or require_gate_opt_in
show_overlay = show_overlay_opt and (role != "public")

# Map
use_leaflet = True
try:
    import folium
    from streamlit_folium import st_folium
except Exception:
    use_leaflet = False

col_map, col_ev = st.columns((2, 1), gap="large")

with col_map:
    st.subheader("Map")
    colors = _role_colors(role)
    assets_fc = _assets_fc(limit)
    overlays_fc = _overlays_fc(limit) if show_overlay else {"features": []}

    click_asset_id: Optional[str] = None

    if use_leaflet:
        m = folium.Map(location=[24.4539, 54.3773], zoom_start=5)

        if show_assets and assets_fc.get("features"):

            def _style(_: Dict[str, Any]) -> Dict[str, Any]:
                return {"color": colors["asset"], "weight": 2, "fillOpacity": 0.2}

            gj = folium.GeoJson(
                data=assets_fc,
                name="Assets",
                style_function=_style,
                highlight_function=lambda x: {"weight": 3},
                tooltip=folium.GeoJsonTooltip(fields=["name", "asset_type"]),
            )
            gj.add_to(m)

        if show_overlay and overlays_fc.get("features"):
            folium.GeoJson(
                data=overlays_fc,
                name="Overlay (250m)",
                style_function=lambda x: {"color": colors["overlay"], "weight": 2},
            ).add_to(m)

        folium.LayerControl(collapsed=False).add_to(m)
        ret = st_folium(
            m,
            width=None,
            height=650,
            returned_objects=["last_object_clicked", "last_clicked"],
        )
        obj = ret.get("last_object_clicked")
        if isinstance(obj, dict):
            props = obj.get("properties") or {}
            if "asset_id" in props:
                click_asset_id = str(props["asset_id"])

        if not click_asset_id and selected_asset:
            click_asset_id = selected_asset
    else:
        import pydeck as pdk

        layers: List[pdk.Layer] = []
        if show_overlay and overlays_fc.get("features"):
            layers.append(
                pdk.Layer(
                    "GeoJsonLayer",
                    overlays_fc,
                    pickable=False,
                    stroked=True,
                    filled=True,
                    get_fill_color=[180, 0, 0, 80] if role == "regulator" else [170, 170, 170, 80],
                    get_line_color=[180, 0, 0] if role == "regulator" else [120, 120, 120],
                    line_width_min_pixels=2 if role == "regulator" else 1,
                )
            )
        if show_assets and assets_fc.get("features"):
            layers.append(
                pdk.Layer(
                    "GeoJsonLayer",
                    assets_fc,
                    pickable=True,
                    stroked=True,
                    filled=True,
                    get_fill_color=[40, 110, 255, 80] if role != "public" else [42, 157, 143, 80],
                    get_line_color=[40, 110, 255],
                    line_width_min_pixels=1,
                )
            )

        deck = pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(latitude=24.4539, longitude=54.3773, zoom=5),
            tooltip={"text": "{name}\n{asset_type}"},
            map_style="mapbox://styles/mapbox/light-v9",
        )
        st.pydeck_chart(deck, use_container_width=True)
        click_asset_id = selected_asset

with col_ev:
    st.subheader("Evidence Panel")
    if not click_asset_id:
        st.info("Click an asset on the map or pick one in the sidebar.")
    else:
        asset_id = click_asset_id.strip()
        meta = fetch_asset_meta(asset_id)
        ev = fetch_evidence_windowed(
            asset_id=asset_id,
            days=days,
            min_citations_flag=2,
            top_k=50,
        )

        # Role policy transforms
        if require_gate:
            ev = [e for e in ev if int(e.get("citation_count") or 0) >= 2]

        if role == "investor":
            ev = sorted(
                ev,
                key=lambda e: _risk_score(e.get("published_at"), int(e.get("citation_count") or 0)),
                reverse=True,
            )[
                :10
            ]  # top by risk

        has_min_any = any((e.get("citation_count") or 0) >= 2 for e in ev)

        # Header
        if meta:
            subtitle = f"**{meta.get('name','(unknown)')}** — {meta.get('asset_type','?')}"
            locbits = ", ".join([v for v in [meta.get("city"), meta.get("country")] if v])
            if locbits:
                subtitle += f"  \n{locbits}"
            st.markdown(subtitle)
        else:
            st.markdown(f"**Asset:** `{asset_id}`")

        # Banner by role/gate
        if not ev:
            st.warning("No events for this selection (after role filters / window).")
        else:
            banner = (
                "✅ Gate passed (≥2 citations in at least one doc)"
                if has_min_any
                else "⚠️ Gate not passed (no doc with ≥2 citations)"
            )
            st.caption(
                f"Theme: **{role}** — {banner}"
                + (" • URLs redacted for public view" if role == "public" else "")
            )

            # Render table per role
            rows: List[Dict[str, Any]] = []
            for e in ev:
                row = {
                    "title": e.get("title") or "",
                    "date": str(e.get("published_at") or "")[:19],
                    "citations": int(e.get("citation_count") or 0),
                }
                if role != "public":
                    row["source"] = e.get("source") or ""
                    row["url"] = e.get("url") or ""
                rows.append(row)

            st.dataframe(rows, use_container_width=True, hide_index=True)

            # --- M5.3 details: doc select + bundle + citations + KG edges ---
            title_to_sha = {}
            for e in ev:
                t = (e.get("title") or "").strip() or "(untitled)"
                title_to_sha[t] = str(e.get("doc_sha256") or "")

            sel_doc_title = st.selectbox(
                "Select a document to inspect",
                options=list(title_to_sha.keys()),
                index=0 if title_to_sha else None,
            )
            sel_sha = title_to_sha.get(sel_doc_title or "")

            if sel_sha:
                bundle_id = fetch_doc_proof_bundle(sel_sha) or "(none)"
                st.markdown(f"**Proof bundle id:** `{bundle_id}`")

                st.markdown("**Citations**")
                cits = fetch_doc_citations(sel_sha, top_k=12)
                if not cits:
                    st.info("No clauses found for this document.")
                else:
                    st.dataframe(cits, use_container_width=True, hide_index=True)

                st.markdown("**Knowledge Graph edges (from clauses)**")
                edges = fetch_kg_edges(asset_id, sel_sha, top_k=12)
                if not edges:
                    st.info("No edges generated for this document.")
                else:
                    st.dataframe(edges, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(
    "Role theming affects filters (gate), sorting, styling, and visibility. "
    "Use Refresh data after DB updates."
)
