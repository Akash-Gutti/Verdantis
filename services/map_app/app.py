from __future__ import annotations

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


# --- helpers ---------------------------------------------------------------
def hard_refresh() -> None:
    """Clear Streamlit caches and rerun (works on new/old Streamlit)."""
    st.cache_data.clear()
    if hasattr(st, "rerun"):
        st.rerun()
    # Fall back for very old versions
    if hasattr(st, "experimental_rerun"):  # pragma: no cover
        st.experimental_rerun()  # type: ignore[attr-defined]


@st.cache_data(show_spinner=False, ttl=30)
def _assets_fc(limit: int) -> Dict[str, Any]:
    return fetch_assets_geojson(limit=limit)


@st.cache_data(show_spinner=False, ttl=30)
def _overlays_fc(limit: int) -> Dict[str, Any]:
    return fetch_overlays_geojson(limit=limit)


@st.cache_data(show_spinner=False, ttl=30)
def _asset_list(limit: int) -> List[tuple[str, str]]:
    return fetch_asset_list(limit=limit)


# --- page -----------------------------------------------------------------
st.set_page_config(
    page_title="Verdantis | Digital Twin (M5.2)",
    layout="wide",
    page_icon="🌍",
)

st.title("🌍 Verdantis — Geospatial Digital Twin (M5.2)")
st.caption(
    "Map + overlays. Evidence panel shows docs & citation counts; " "role theming toggles filters."
)

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    role = st.radio("Role", ["regulator", "investor", "public"], index=0)

    # YEARS slider (1–5) → convert to days
    years = st.slider("Event window (years)", 1, 5, 1, step=1)
    days = int(years) * 365

    # Gate view default: regulators require ≥2 citations
    require_gate = st.checkbox(
        "Require ≥2 citations (gate view)",
        value=(role == "regulator"),
    )

    limit = st.slider("Max features", 50, 1000, 300, step=50)
    show_overlay = st.checkbox("Show proximity overlay (250m)", value=True)
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

# Try Leaflet via streamlit-folium first
use_leaflet = True
try:
    import folium
    from streamlit_folium import st_folium
except Exception:
    use_leaflet = False

col_map, col_ev = st.columns((2, 1), gap="large")

with col_map:
    st.subheader("Map")
    assets_fc = _assets_fc(limit)
    overlays_fc = _overlays_fc(limit) if show_overlay else {"features": []}

    click_asset_id: Optional[str] = None

    if use_leaflet:
        m = folium.Map(location=[24.4539, 54.3773], zoom_start=5)

        if show_assets and assets_fc.get("features"):

            def _style(_: Dict[str, Any]) -> Dict[str, Any]:
                return {"color": "#3388ff", "weight": 2, "fillOpacity": 0.2}

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
                style_function=lambda x: {"color": "#999999", "weight": 1},
            ).add_to(m)

        folium.LayerControl(collapsed=False).add_to(m)
        ret = st_folium(
            m,
            width=None,
            height=650,
            returned_objects=["last_object_clicked", "last_clicked"],
        )

        # Note: some environments do not return feature properties on click.
        # Sidebar selection is the reliable path.
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
                    get_fill_color=[200, 200, 200],
                    get_line_color=[120, 120, 120],
                    line_width_min_pixels=1,
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
                    get_fill_color=[40, 110, 255, 80],
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
            days=days,  # 1–5 year window
            min_citations_flag=2,
            top_k=20,
        )
        has_min_any = any((e.get("citation_count") or 0) >= 2 for e in ev)

        # Header with basic meta
        if meta:
            subtitle = f"**{meta.get('name','(unknown)')}** — {meta.get('asset_type','?')}"
            locbits = ", ".join([v for v in [meta.get("city"), meta.get("country")] if v])
            if locbits:
                subtitle += f"  \n{locbits}"
            st.markdown(subtitle)
        else:
            st.markdown(f"**Asset:** `{asset_id}`")

        # Gate status
        if not ev:
            st.warning(f"No events for this window ({years} year(s)).")
        else:
            st.caption(
                "✅ Gate passed (≥2 citations in at least one doc)"
                if has_min_any
                else "⚠️ Gate not passed (no doc with ≥2 citations)"
            )

            rows: List[Dict[str, Any]] = []
            for e in ev:
                if require_gate and (e.get("citation_count") or 0) < 2:
                    continue
                rows.append(
                    {
                        "title": e.get("title") or "",
                        "date": str(e.get("published_at") or "")[:19],
                        "source": e.get("source") or "",
                        "citations": int(e.get("citation_count") or 0),
                        "url": e.get("url") or "",
                    }
                )
            if not rows and ev:
                st.info("No docs meet the ≥2 citations filter.")
            else:
                st.dataframe(rows, use_container_width=True, hide_index=True)

            # --- M5.3: doc selection + details ---
            # Build a small selector list (title → doc_sha256)
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
                # Proof bundle id
                bundle_id = fetch_doc_proof_bundle(sel_sha) or "(none)"
                st.markdown(f"**Proof bundle id:** `{bundle_id}`")

                # Citations / clauses
                st.markdown("**Citations**")
                cits = fetch_doc_citations(sel_sha, top_k=12)
                if not cits:
                    st.info("No clauses found for this document.")
                else:
                    st.dataframe(
                        cits,
                        use_container_width=True,
                        hide_index=True,
                    )

                # KG edges
                st.markdown("**Knowledge Graph edges (from clauses)**")
                edges = fetch_kg_edges(asset_id, sel_sha, top_k=12)
                if not edges:
                    st.info("No edges generated for this document.")
                else:
                    st.dataframe(
                        edges,
                        use_container_width=True,
                        hide_index=True,
                    )

st.markdown("---")
st.caption(
    "Tip: If map clicks don’t select an asset, use the sidebar selector. "
    "The refresh button clears caches and reruns the app."
)
