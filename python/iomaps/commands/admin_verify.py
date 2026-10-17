"""Check deployment and live route bytes after a dataset publication."""

import gzip
import hashlib
import json
import math
import re
import subprocess
import time

import click
import requests


def pmtiles(*args):
    try:
        result = subprocess.run(["pmtiles", *args], capture_output=True, check=True)
    except FileNotFoundError as exc:
        raise click.ClickException("Live verification requires pmtiles on PATH") from exc
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(f"pmtiles failed: {exc.stderr.decode(errors='replace')}") from exc
    return result.stdout


def decoded(data):
    return gzip.decompress(data) if data.startswith(b"\x1f\x8b") else data


def sample_manifest(sample):
    """Carry only a tile's checksum/metadata between hosts, never the large dataset."""
    return {
        **{key: value for key, value in sample.items() if key != "data"},
        "data_sha256": hashlib.sha256(sample["data"]).hexdigest(),
        "data_bytes": len(sample["data"]),
    }


def validate_sample_manifest(sample):
    try:
        z, x, y = (sample[key] for key in ("z", "x", "y"))
        if not all(isinstance(value, int) for value in (z, x, y)) or not 0 <= z <= 30:
            raise ValueError("invalid tile coordinates")
        if not 0 <= x < 2**z or not 0 <= y < 2**z:
            raise ValueError("invalid tile coordinates")
        if not re.fullmatch(r"[0-9a-f]{64}", sample["data_sha256"]) or sample["data_bytes"] <= 0:
            raise ValueError("invalid tile checksum/size")
        if not sample["layers"] or any(not layer["id"] for layer in sample["layers"]):
            raise ValueError("missing tile layers")
    except (KeyError, TypeError, ValueError) as exc:
        raise click.ClickException(
            "Receipt has no valid sample tile; rerun --phase assets with verification enabled"
        ) from exc


def choose_sample_tile(plan):
    """Find a nonempty local tile before uploads, including in a partitioned dataset."""
    entry = next(iter(plan["route_entry"].values()))
    for asset in plan["uploads"]:
        if not asset["name"].endswith(".pmtiles"):
            continue
        header = json.loads(pmtiles("show", asset["path"], "--header-json"))
        metadata = json.loads(pmtiles("show", asset["path"], "--metadata"))
        layers = metadata.get("vector_layers", [])
        field = entry.get("promoteid")
        if field and (not layers or any(field not in layer.get("fields", {}) for layer in layers)):
            raise click.ClickException(
                f"promoteid {field!r} is absent from the tiles in {asset['name']}; "
                "choose a source field with --promoteid and generate a new preview"
            )
        lon, lat, zoom = header["center"]
        lat = max(-85.05112878, min(85.05112878, lat))
        for z in range(min(int(zoom), header["maxzoom"]), header["minzoom"] - 1, -1):
            n = 2**z
            x = max(0, min(n - 1, int((lon + 180) / 360 * n)))
            y = max(0, min(n - 1, int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n)))
            data = pmtiles("tile", asset["path"], str(z), str(x), str(y))
            if data:
                return {"z": z, "x": x, "y": y, "data": decoded(data), "layers": layers, "header": header}
    raise click.ClickException("Could not find a nonempty local sample tile; live verification cannot be prepared")


def verify_live(plan, sha, sample, timeout):
    if "data" in sample:
        sample = sample_manifest(sample)
    validate_sample_manifest(sample)
    route, entry = next(iter(plan["route_entry"].items()))
    base = "https://indianopenmaps.com"
    deadline = time.monotonic() + timeout
    last_error = "Deployment check has not appeared"

    def get(url):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise requests.Timeout("verification deadline reached")
        response = requests.get(url, timeout=min(15, remaining))
        response.raise_for_status()
        return response

    while time.monotonic() < deadline:
        try:
            checks = get(
                f"https://api.github.com/repos/{plan['route_repository']}/commits/{sha}/check-runs?per_page=100"
            ).json()["check_runs"]
            builds = [c for c in checks if c["name"] == "Workers Builds: indianopenmaps"]
            build = max(builds, key=lambda c: c["id"]) if builds else None
            if build and build["status"] == "completed" and build["conclusion"] != "success":
                raise click.ClickException(f"Cloudflare build {build['conclusion']}: {build.get('details_url', '')}")
            if not build or build["status"] != "completed":
                last_error = "waiting for Cloudflare's build"
            elif get(f"{base}/api/routes").json().get(route) != entry:
                last_error = "live route has not matched the plan yet"
            else:
                tilejson = get(f"{base}{route}tiles.json").json()
                expected_layers = {layer["id"] for layer in sample["layers"]}
                actual_layers = {layer["id"] for layer in tilejson.get("vector_layers", [])}
                tile_urls = tilejson.get("tiles", [])
                valid_tiles = tile_urls and all(url.startswith(f"{base}{route}") for url in tile_urls)
                valid_zooms = tilejson.get("minzoom", 99) <= sample["z"] <= tilejson.get("maxzoom", -1)
                if not expected_layers.issubset(actual_layers) or not valid_tiles or not valid_zooms:
                    last_error = "live TileJSON does not match the local tiles"
                else:
                    url = f"{base}{route}{sample['z']}/{sample['x']}/{sample['y']}.pbf"
                    data = decoded(get(url).content)
                    if len(data) == sample["data_bytes"] and hashlib.sha256(data).hexdigest() == sample["data_sha256"]:
                        return {
                            "status": "verified",
                            "viewer_url": f"{base}{route}view",
                            "build_url": build.get("details_url"),
                            "tile_url": url,
                            "tile_sha256": hashlib.sha256(data).hexdigest(),
                            "route_matches": True,
                            "tilejson_matches": True,
                            "tile_matches_local": True,
                        }
                    last_error = "live sample tile differs from the local tile"
        except (requests.RequestException, ValueError, KeyError, OSError) as exc:
            last_error = str(exc)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            click.echo(f"Published; {last_error}. Checking again...")
            time.sleep(min(10, remaining))
    raise click.ClickException(
        f"Published, but live verification timed out: {last_error}. "
        "Retry the same plan to verify again without duplicating the publication."
    )
