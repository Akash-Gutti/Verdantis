"""M6.4 Overlay generator: raster mask -> GeoJSON polygons (AABB).

For each AOI under data/interim/m6/<aoi_id>/:
- Read manifest.json (for bbox + mode) and change_mask.tif (uint8 0/1)
- Label connected components, compute axis-aligned bounding boxes (AABB)
- Project pixel boxes into lon/lat using manifest["bbox"] (minx,miny,maxx,maxy)
- Write per-AOI GeoJSON FeatureCollection at:
    data/processed/overlays/m6_<aoi_id>_changes.geojson
- Write aggregated GeoJSON at:
    data/processed/overlays/m6_changes.geojson
- Write summary JSON with counts

Notes:
- We assume the mask image spans the manifest bbox exactly and is north-up.
- Pixel (row=0) corresponds to max latitude (top of bbox).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from math import cos, pi
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from scipy import ndimage as ndi

try:
    import tifffile as tiff
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("tifffile not installed. Run: pip install tifffile") from exc


OVERLAY_DIR = Path("data/processed/overlays")


@dataclass
class OverlayConfig:
    min_area_px: int = 64  # skip tiny speckles
    connectivity: int = 1  # 1: 4-neighbourhood; 2: 8-neighbourhood (ndi uses 1/2)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _load_json(p: Path) -> Dict:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(p: Path, data: Dict) -> None:
    _ensure_dir(p.parent)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _pixel_box_to_lonlat(
    x0: int,
    y0: int,
    x1: int,
    y1: int,
    width: int,
    height: int,
    bbox_ll: Tuple[float, float, float, float],
) -> List[List[float]]:
    """Map pixel AABB [x0:x1, y0:y1] to lon/lat polygon (closed ring)."""
    minx, miny, maxx, maxy = bbox_ll
    dx = maxx - minx
    dy = maxy - miny

    # Pixel -> geo; note rows increase downward, so lat decreases with y.
    def px_to_lon(x: float) -> float:
        return minx + (x / float(width)) * dx

    def px_to_lat(y: float) -> float:
        return maxy - (y / float(height)) * dy

    # Clamp to image bounds
    x0c = max(0, min(int(x0), width))
    x1c = max(0, min(int(x1), width))
    y0c = max(0, min(int(y0), height))
    y1c = max(0, min(int(y1), height))

    lon0 = px_to_lon(x0c)
    lon1 = px_to_lon(x1c)
    lat0 = px_to_lat(y0c)
    lat1 = px_to_lat(y1c)

    # Build a clockwise ring (right-hand rule for lon/lat is commonly used)
    return [
        [lon0, lat0],
        [lon1, lat0],
        [lon1, lat1],
        [lon0, lat1],
        [lon0, lat0],
    ]


def _approx_area_km2(lon0: float, lat0: float, lon1: float, lat1: float) -> float:
    """Approximate area of bbox (lon/lat degrees) in km^2."""
    lat_mid = 0.5 * (lat0 + lat1)
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * cos(lat_mid * pi / 180.0)
    w_km = abs(lon1 - lon0) * km_per_deg_lon
    h_km = abs(lat1 - lat0) * km_per_deg_lat
    return float(w_km * h_km)


def _label_components(mask: np.ndarray, conn: int) -> Tuple[np.ndarray, int]:
    structure = None
    if conn == 2:
        structure = np.ones((3, 3), dtype=bool)
    labeled, nlab = ndi.label(mask.astype(bool), structure=structure)
    return labeled, int(nlab)


def _component_boxes(
    labeled: np.ndarray, nlab: int, min_area_px: int
) -> List[Tuple[int, int, int, int, int]]:
    """Return list of (label, x0, y0, x1, y1, area_px)."""
    boxes: List[Tuple[int, int, int, int, int]] = []
    # Use find_objects to get bounding slices
    slices = ndi.find_objects(labeled)
    if not slices:
        return boxes

    for lab_id, slc in enumerate(slices, start=1):
        if slc is None:
            continue
        ys, xs = slc
        sub = labeled[ys, xs] == lab_id
        area = int(np.count_nonzero(sub))
        if area < int(min_area_px):
            continue
        x0 = int(xs.start)
        x1 = int(xs.stop)
        y0 = int(ys.start)
        y1 = int(ys.stop)
        boxes.append((lab_id, x0, y0, x1, y1, area))
    return boxes


def _build_features(
    aoi_id: str,
    bbox_ll: Tuple[float, float, float, float],
    mask_shape: Tuple[int, int],
    boxes: List[Tuple[int, int, int, int, int]],
    aoi_props: Dict[str, object],
) -> List[Dict]:
    feats: List[Dict] = []
    height, width = mask_shape

    for idx, (lab_id, x0, y0, x1, y1, area_px) in enumerate(boxes):
        ring = _pixel_box_to_lonlat(x0, y0, x1, y1, width, height, bbox_ll)
        lon0, lat0 = ring[0]
        lon1, lat1 = ring[2]
        frac_img = float(area_px) / float(width * height) if width * height else 0.0

        props = {
            "aoi_id": aoi_id,
            "component_id": idx + 1,
            "label_id": lab_id,
            "area_px": area_px,
            "area_frac_image": frac_img,
            "bbox_px": [x0, y0, x1, y1],
            "bbox_lonlat": [lon0, lat0, lon1, lat1],
        }
        # carry over a few AOI-level props (e.g., mode, change_score, severity)
        props.update(aoi_props)

        feat = {
            "type": "Feature",
            "properties": props,
            "geometry": {
                "type": "Polygon",
                "coordinates": [ring],
            },
        }
        feats.append(feat)
    return feats


def _aoi_level_props(manifest: Dict, metrics: Dict) -> Dict[str, object]:
    score = float(metrics.get("change_score", 0.0))
    severity = "low"
    if score >= 0.05:
        severity = "high"
    elif score >= 0.01:
        severity = "medium"

    return {
        "mode": manifest.get("mode"),
        "percentile": metrics.get("percentile"),
        "threshold": metrics.get("threshold"),
        "change_score": score,
        "severity": severity,
    }


def process_aoi_overlay(aoi_id: str, cfg: OverlayConfig) -> Dict:
    """Create overlay GeoJSON for a single AOI if change_mask exists."""
    aoi_dir = Path("data/interim/m6") / aoi_id
    manifest_p = aoi_dir / "manifest.json"
    metrics_p = aoi_dir / "metrics.json"
    mask_p = aoi_dir / "change_mask.tif"

    if not (manifest_p.exists() and metrics_p.exists() and mask_p.exists()):
        return {"aoi_id": aoi_id, "features": 0, "skipped": True}

    manifest = _load_json(manifest_p)
    metrics = _load_json(metrics_p)
    bbox = manifest.get("bbox")
    if not bbox or len(bbox) != 4:
        return {"aoi_id": aoi_id, "features": 0, "skipped": True}

    mask = tiff.imread(str(mask_p))
    if mask.ndim != 2:
        # If mask has channels, take first
        mask = np.asarray(mask)[..., 0]
    mask = (mask > 0).astype(np.uint8)

    labeled, nlab = _label_components(mask, cfg.connectivity)
    boxes = _component_boxes(labeled, nlab, cfg.min_area_px)

    aoi_props = _aoi_level_props(manifest, metrics)
    feats = _build_features(
        aoi_id=aoi_id,
        bbox_ll=(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])),
        mask_shape=mask.shape,
        boxes=boxes,
        aoi_props=aoi_props,
    )

    fc = {"type": "FeatureCollection", "features": feats}
    out_path = OVERLAY_DIR / f"m6_{aoi_id}_changes.geojson"
    _save_json(out_path, fc)
    return {"aoi_id": aoi_id, "features": len(feats), "skipped": False}


def run_m6_4(min_area_px: int | None = None, connectivity: int | None = None) -> Dict:
    """Run overlay generation for all AOIs in data/interim/m6/index.json."""
    index_p = Path("data/interim/m6/index.json")
    if not index_p.exists():
        raise FileNotFoundError("Run M6.1 first (missing index.json).")
    index = _load_json(index_p)

    cfg = OverlayConfig(
        min_area_px=int(min_area_px) if min_area_px is not None else 64,
        connectivity=int(connectivity) if connectivity is not None else 1,
    )

    _ensure_dir(OVERLAY_DIR)

    per_aoi_summ: List[Dict] = []
    agg_feats: List[Dict] = []

    for aoi_id in index.keys():
        summ = process_aoi_overlay(aoi_id, cfg)
        per_aoi_summ.append(summ)

        # Append features to aggregate if file exists
        out_path = OVERLAY_DIR / f"m6_{aoi_id}_changes.geojson"
        if out_path.exists():
            fc = _load_json(out_path)
            agg_feats.extend(fc.get("features", []))

    # Write aggregate FC + summary
    agg = {"type": "FeatureCollection", "features": agg_feats}
    _save_json(OVERLAY_DIR / "m6_changes.geojson", agg)

    meta = {
        "aois": per_aoi_summ,
        "total_features": int(sum(x.get("features", 0) for x in per_aoi_summ)),
    }
    _save_json(OVERLAY_DIR / "m6_overlay_summary.json", meta)
    return meta
