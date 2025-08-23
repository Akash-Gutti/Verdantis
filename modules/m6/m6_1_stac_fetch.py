"""M6.1 STAC-like fetch/resolve (local-first, slug-aware).

Validates and stages input imagery for each AOI into data/interim/m6/<aoi_id>/,
and writes a manifest.json describing resolved file paths and dates.

Input modes supported:
1) Slug mode (recommended for your dataset):
   - Provide {"slug": "<aoi_slug>", "raw_dir": ".../verdantis_satellite_exports"}
   - Files expected in raw_dir:
       {slug}_before_B4.tif,  {slug}_before_B8.tif,
       {slug}_after_B4.tif,   {slug}_after_B8.tif
   - B04/B8 synonyms handled (B4↔B04)
2) Explicit files mode:
   - Provide {"files": {...}} with keys:
       NDVI: before_B4/before_B8/after_B4/after_B8 (or B04/B08)
       or RGB: before_rgb/after_rgb (.tif/.tiff/.png/.jpg/.jpeg allowed)

Staged filenames are canonicalized to:
- NDVI: before_B4.tif, before_B8.tif, after_B4.tif, after_B8.tif
- RGB:  before_rgb.tif, after_rgb.tif
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

IMG_EXTS = (".tif", ".tiff", ".png", ".jpg", ".jpeg")


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")


def _first_existing(paths: Iterable[Path]) -> Optional[Path]:
    for p in paths:
        if p.exists():
            return p
    return None


def _stage_file(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.resolve() != dst.resolve():
        shutil.copyfile(src, dst)
    return str(dst.as_posix())


def _candidate_names_for_band(slug: str, timing: str, band: str) -> List[str]:
    """Return possible filenames under the slug pattern.

    Examples:
      timing in {"before", "after"}
      band in {"B4", "B8"}; we also try B04 for safety.
    """
    band_alts = [band]
    if band == "B4":
        band_alts.append("B04")
    elif band == "B8":
        band_alts.append("B08")

    names: List[str] = []
    for b in band_alts:
        names.append(f"{slug}_{timing}_{b}.tif")
        names.append(f"{slug}_{timing}_{b}.tiff")
    return names


def _resolve_ndvi_paths_slug(raw_dir: Path, slug: str) -> Dict[str, Path]:
    """Resolve NDVI paths for a slug. Raises if not all are found."""
    pairs = {
        "before_B4": _candidate_names_for_band(slug, "before", "B4"),
        "before_B8": _candidate_names_for_band(slug, "before", "B8"),
        "after_B4": _candidate_names_for_band(slug, "after", "B4"),
        "after_B8": _candidate_names_for_band(slug, "after", "B8"),
    }
    resolved: Dict[str, Path] = {}
    for key, names in pairs.items():
        candidates = [raw_dir / n for n in names]
        found = _first_existing(candidates)
        if not found:
            raise FileNotFoundError(
                f"Could not find any of: {', '.join(str(c) for c in candidates)}"
            )
        resolved[key] = found
    return resolved


def _resolve_explicit(
    raw_dir: Path,
    files: Dict[str, str],
) -> Tuple[str, Dict[str, Path]]:
    """Return ('ndvi'|'rgb', resolved_paths)."""
    # Accept both B4/B8 and B04/B08 keys in config.
    # Normalize to B4/B8 internally.
    key_aliases = {
        "before_B4": ["before_B4", "before_B04"],
        "before_B8": ["before_B8", "before_B08"],
        "after_B4": ["after_B4", "after_B04"],
        "after_B8": ["after_B8", "after_B08"],
    }

    def pick(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in files:
                return files[k]
        return None

    # Try NDVI first
    ndvi_map: Dict[str, Optional[str]] = {
        "before_B4": pick(key_aliases["before_B4"]),
        "before_B8": pick(key_aliases["before_B8"]),
        "after_B4": pick(key_aliases["after_B4"]),
        "after_B8": pick(key_aliases["after_B8"]),
    }
    if all(ndvi_map.values()):
        paths = {k: raw_dir / v for k, v in ndvi_map.items() if v is not None}
        for p in paths.values():
            _require_exists(p)
        return "ndvi", paths

    # Else RGB
    if "before_rgb" in files and "after_rgb" in files:
        paths = {
            "before_rgb": raw_dir / files["before_rgb"],
            "after_rgb": raw_dir / files["after_rgb"],
        }
        for p in paths.values():
            # Try flexible extensions if given path missing
            if not p.exists():
                base = p.with_suffix("")
                alt = _first_existing(base.with_suffix(ext) for ext in IMG_EXTS)
                if not alt:
                    _require_exists(p)
                else:
                    p = alt  # type: ignore[assignment]
        # Recreate dict with resolved paths (p may be updated)
        paths = {k: Path(v) for k, v in paths.items()}
        return "rgb", paths

    raise ValueError(
        "Explicit files must define either NDVI "
        "(before_B4/B8, after_B4/B8) or RGB (before_rgb/after_rgb)."
    )


def resolve_and_stage_aoi(
    aoi_cfg: Dict[str, object],
    interim_root: Path,
) -> Tuple[str, Dict[str, object]]:
    """Validate inputs for one AOI and copy to interim, returning manifest.

    Returns:
        (aoi_id, manifest_dict)
    """
    aoi_id = str(aoi_cfg["id"])
    raw_dir = Path(str(aoi_cfg["raw_dir"]))
    aoi_dir = interim_root / aoi_id
    aoi_dir.mkdir(parents=True, exist_ok=True)

    # Choose mode: slug or explicit 'files'
    mode: Optional[str] = None
    resolved_files: Dict[str, Path] = {}

    if "slug" in aoi_cfg:
        slug = str(aoi_cfg["slug"])
        resolved_files = _resolve_ndvi_paths_slug(raw_dir, slug)
        mode = "ndvi"
    elif "files" in aoi_cfg:
        files = dict(aoi_cfg.get("files", {}))
        mode, resolved_files = _resolve_explicit(raw_dir, files)
    else:
        raise ValueError(f"AOI {aoi_id} must specify either 'slug' or 'files' in the config.")

    manifest: Dict[str, object] = {
        "aoi_id": aoi_id,
        "bbox": aoi_cfg.get("bbox", None),
        "before_date": aoi_cfg.get("before_date"),
        "after_date": aoi_cfg.get("after_date"),
        "mode": mode,
        "files": {},
    }

    if mode == "ndvi":
        mapping = [
            ("before_B4", "before_B4.tif"),
            ("before_B8", "before_B8.tif"),
            ("after_B4", "after_B4.tif"),
            ("after_B8", "after_B8.tif"),
        ]
    else:
        mapping = [("before_rgb", "before_rgb.tif"), ("after_rgb", "after_rgb.tif")]

    for key, dst_name in mapping:
        src = resolved_files[key]
        staged = _stage_file(src, aoi_dir / dst_name)
        manifest["files"][key] = staged

    # Write manifest.json
    with (aoi_dir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return aoi_id, manifest


def run_m6_1(config_path: str) -> List[Tuple[str, Dict[str, object]]]:
    """Entry point for M6.1.

    Args:
        config_path: Path to configs/m6_satellite.json

    Returns:
        List of (aoi_id, manifest_dict)
    """
    cfg_p = Path(config_path)
    if not cfg_p.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    with cfg_p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    aois = cfg.get("aois", [])
    if not aois:
        raise ValueError("No AOIs configured in m6_satellite.json")

    interim_root = Path("data/interim/m6")
    results: List[Tuple[str, Dict[str, object]]] = []

    for aoi in aois:
        aoi_id, manifest = resolve_and_stage_aoi(aoi, interim_root)
        results.append((aoi_id, manifest))

    # Index file for convenience
    index = {
        aoi_id: {"manifest": str((Path("data/interim/m6") / aoi_id / "manifest.json").as_posix())}
        for aoi_id, _ in results
    }
    with (interim_root / "index.json").open("w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)

    return results
