"""M6.2 Differencing + thresholding + morphology.

Reads per-AOI manifest.json from data/interim/m6/<aoi_id>/ produced by M6.1,
computes NDVI (preferred) or grayscale RGB differencing (fallback),
thresholds by percentile, applies morphology, and writes:
- ndvi_diff.tif or rgb_diff.tif
- change_mask.tif (uint8)
- metrics.json (summary with change_score in [0,1])

Threshold strategy:
- Compute a score image (|ndvi_after - ndvi_before| for mode='abs').
- Select threshold at given percentile (e.g., 97.5).
- Morph: opening, closing, fill holes, and remove small components.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
from scipy import ndimage as ndi

try:
    import tifffile as tiff
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("tifffile not installed. Run: pip install tifffile") from exc

try:
    from PIL import Image
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("Pillow not installed. Run: pip install pillow") from exc


EPS = 1e-6


@dataclass
class DiffConfig:
    percentile: float = 97.5
    min_area: int = 64
    open_iters: int = 1
    close_iters: int = 1
    mode: str = "abs"  # "abs" | "neg" | "pos"


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _read_any(path: Path) -> np.ndarray:
    """Robust reader for staged files; tries tifffile then Pillow."""
    try:
        arr = tiff.imread(str(path))
        return np.asarray(arr)
    except Exception:
        # Fall back to PIL (handles PNG/JPG even with .tif extension)
        with Image.open(str(path)) as im:
            return np.asarray(im)


def _to_float01(arr: np.ndarray) -> np.ndarray:
    """Scale array to [0,1] when possible."""
    arr = np.asarray(arr)
    if arr.dtype == np.uint16 or float(arr.max(initial=0)) > 1.5:
        return arr.astype(np.float32) / 10000.0
    if arr.dtype == np.uint8:
        return arr.astype(np.float32) / 255.0
    arr = arr.astype(np.float32)
    if arr.max(initial=0) > 1.0 + 1e-3:  # arbitrary safety
        m = arr.max(initial=1.0)
        return arr / float(m)
    return arr


def _to_gray01(arr: np.ndarray) -> np.ndarray:
    """Convert RGB or single-band to [0,1] grayscale."""
    arr = np.asarray(arr)
    if arr.ndim == 3 and arr.shape[2] >= 3:
        r = _to_float01(arr[:, :, 0])
        g = _to_float01(arr[:, :, 1])
        b = _to_float01(arr[:, :, 2])
        return (0.2989 * r + 0.5870 * g + 0.1140 * b).astype(np.float32)
    if arr.ndim == 2:
        return _to_float01(arr)
    # Fallback for unexpected shapes: take first channel
    return _to_float01(arr[..., 0])


def _compute_ndvi(nir: np.ndarray, red: np.ndarray) -> np.ndarray:
    nirf = _to_float01(nir)
    redf = _to_float01(red)
    denom = np.maximum(nirf + redf, EPS)
    return ((nirf - redf) / denom).astype(np.float32)


def _remove_small(mask: np.ndarray, min_area: int) -> np.ndarray:
    if min_area <= 1:
        return mask
    labels, nlab = ndi.label(mask)
    if nlab == 0:
        return mask
    counts = np.bincount(labels.ravel())
    remove_ids = np.where(counts < min_area)[0]
    if 0 in remove_ids:
        remove_ids = remove_ids[remove_ids != 0]
    if remove_ids.size == 0:
        return mask
    # build a boolean index for labels to remove
    drop = np.isin(labels, remove_ids)
    mask[drop] = False
    return mask


def _morph(mask: np.ndarray, open_iters: int, close_iters: int) -> np.ndarray:
    out = mask
    if open_iters > 0:
        out = ndi.binary_opening(out, iterations=int(open_iters))
    if close_iters > 0:
        out = ndi.binary_closing(out, iterations=int(close_iters))
    out = ndi.binary_fill_holes(out)
    return out


def _ndvi_pipeline(manifest_dir: Path, cfg: DiffConfig) -> Tuple[np.ndarray, np.ndarray]:
    before_b4 = _read_any(manifest_dir / "before_B4.tif")
    before_b8 = _read_any(manifest_dir / "before_B8.tif")
    after_b4 = _read_any(manifest_dir / "after_B4.tif")
    after_b8 = _read_any(manifest_dir / "after_B8.tif")

    ndvi_before = _compute_ndvi(nir=before_b8, red=before_b4)
    ndvi_after = _compute_ndvi(nir=after_b8, red=after_b4)

    diff = (ndvi_after - ndvi_before).astype(np.float32)
    # Save raw diff for inspection
    tiff.imwrite(str(manifest_dir / "ndvi_diff.tif"), diff, dtype=np.float32)

    if cfg.mode == "neg":
        score = np.clip(-diff, 0.0, 1.0)
    elif cfg.mode == "pos":
        score = np.clip(diff, 0.0, 1.0)
    else:
        score = np.abs(diff)

    return score, diff


def _rgb_pipeline(manifest_dir: Path) -> Tuple[np.ndarray, np.ndarray]:
    before = _read_any(manifest_dir / "before_rgb.tif")
    after = _read_any(manifest_dir / "after_rgb.tif")

    g_before = _to_gray01(before)
    g_after = _to_gray01(after)

    diff = (g_after - g_before).astype(np.float32)
    score = np.abs(diff)
    tiff.imwrite(str(manifest_dir / "rgb_diff.tif"), diff, dtype=np.float32)
    return score, diff


def _mask_from_score(score: np.ndarray, cfg: DiffConfig) -> Tuple[np.ndarray, float]:
    vals = score[np.isfinite(score)]
    if vals.size == 0:
        return np.zeros_like(score, dtype=bool), float("nan")
    thr = float(np.percentile(vals, cfg.percentile))
    mask = score >= thr
    return mask, thr


def process_aoi(manifest_path: Path, cfg: DiffConfig) -> Dict:
    manifest_dir = manifest_path.parent
    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    mode = str(manifest.get("mode", "ndvi"))
    if mode not in {"ndvi", "rgb"}:
        raise ValueError(f"Unknown mode in manifest: {mode}")

    if mode == "ndvi":
        score, diff = _ndvi_pipeline(manifest_dir, cfg)
    else:
        score, diff = _rgb_pipeline(manifest_dir)

    mask, thr = _mask_from_score(score, cfg)

    mask = _morph(mask, cfg.open_iters, cfg.close_iters)
    mask = _remove_small(mask, cfg.min_area)

    # Save mask as uint8
    tiff.imwrite(
        str(manifest_dir / "change_mask.tif"),
        mask.astype(np.uint8),
        dtype=np.uint8,
    )

    changed = int(mask.sum())
    total = int(mask.size)
    frac = float(changed / total) if total > 0 else 0.0
    metrics = {
        "aoi_id": manifest.get("aoi_id"),
        "mode": mode,
        "percentile": cfg.percentile,
        "threshold": thr,
        "changed_pixels": changed,
        "total_pixels": total,
        "fraction_changed": frac,
        "change_score": frac,
    }
    _save_json(manifest_dir / "metrics.json", metrics)
    return metrics


def run_m6_2(
    config_path: str,
    percentile: float | None = None,
    min_area: int | None = None,
    open_iters: int | None = None,
    close_iters: int | None = None,
    mode: str | None = None,
) -> Dict[str, Dict]:
    """Run M6.2 for all AOIs listed in data/interim/m6/index.json.

    Config defaults from configs/m6_satellite.json can be overridden by args.
    """
    cfg_p = Path(config_path)
    cfg_json = _load_json(cfg_p) if cfg_p.exists() else {}
    defaults = cfg_json.get("defaults", {})

    cfg = DiffConfig(
        percentile=float(
            percentile if percentile is not None else defaults.get("percentile", 97.5)
        ),
        min_area=int(min_area if min_area is not None else defaults.get("min_area", 64)),
        open_iters=int(open_iters if open_iters is not None else defaults.get("open_iters", 1)),
        close_iters=int(close_iters if close_iters is not None else defaults.get("close_iters", 1)),
        mode=str(mode if mode is not None else defaults.get("mode", "abs")),
    )

    index_path = Path("data/interim/m6/index.json")
    if not index_path.exists():
        raise FileNotFoundError("Index not found: data/interim/m6/index.json. " "Run M6.1 first.")
    index = _load_json(index_path)

    results: Dict[str, Dict] = {}
    for aoi_id, entry in index.items():
        manifest_path = Path(entry["manifest"])
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest missing for {aoi_id}: {manifest_path}")
        metrics = process_aoi(manifest_path, cfg)
        results[aoi_id] = metrics

    # Aggregate summary at module level
    agg = {
        "num_aois": len(results),
        "avg_change_score": (
            float(np.mean([m["change_score"] for m in results.values()]))  # noqa: E501
            if results
            else 0.0
        ),
    }
    _save_json(Path("data/interim/m6/summary.json"), agg)
    return results
