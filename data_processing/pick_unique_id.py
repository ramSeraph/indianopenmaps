#!/usr/bin/env python3
"""
Analyze field_analysis JSON files and pick the best unique ID field for each dataset.

Selection rules:
1. If unique fields exist, prefer one with "id" in the name (case insensitive)
2. If no unique fields, pick the one with highest uniqueness ratio above threshold

Usage:
    python pick_unique_id.py [--output results.json] [--threshold 0.95]
"""

import argparse
import json
from pathlib import Path


def score_field_name(name: str) -> int:
    """Score a field name for ID-likeness. Higher is better. Returns None to exclude."""
    name_lower = name.lower()
    
    # Exclude shape/geometry derived fields entirely
    shape_patterns = ["shape", "length", "area", "perimeter", "centroid", "st_area", "st_length", "starea", "stlength"]
    if any(p in name_lower for p in shape_patterns):
        return None  # Exclude from consideration
    
    # Exact matches get highest scores
    if name_lower == "id":
        return 100
    if name_lower == "uid" or name_lower == "uuid":
        return 95
    
    # Fields ending with _id or id
    if name_lower.endswith("_id") or name_lower.endswith("id"):
        return 80
    
    # Fields starting with id
    if name_lower.startswith("id_") or name_lower.startswith("id"):
        return 75
    
    # Fields containing id
    if "id" in name_lower:
        return 60
    
    # Code fields are often good identifiers
    if "code" in name_lower:
        return 50
    
    # Key fields
    if "key" in name_lower:
        return 45
    
    # Name fields can be unique but less preferred
    if "name" in name_lower:
        return 20
    
    # Avoid internal fields
    if any(x in name_lower for x in ["objectid", "fid", "inpoly", "simpgn", "simptol"]):
        return -5
    
    return 0


def pick_best_id(analysis: dict, threshold: float = 0.95) -> dict:
    """
    Pick the best unique ID field from analysis results.
    
    Returns dict with:
        - field: chosen field name (or None)
        - is_unique: whether field is truly unique
        - uniqueness_ratio: distinct_values / total_features
        - reason: explanation for choice
        - best_below_threshold: info about best field if none met threshold
    """
    fields = analysis.get("fields", {})
    total = analysis.get("total_features", 0)
    
    if not fields or total == 0:
        return {"field": None, "reason": "no fields or no features"}
    
    # Separate unique and non-unique fields
    unique_fields = []
    candidate_fields = []
    all_fields = []
    
    for name, info in fields.items():
        if isinstance(info, dict) and "error" not in info:
            name_score = score_field_name(name)
            
            # Skip excluded fields (shape/area/length)
            if name_score is None:
                continue
            
            distinct = info.get("distinct_values", 0)
            null_count = info.get("null_count", 0)
            is_unique = info.get("is_unique", False)
            ratio = distinct / total if total > 0 else 0
            
            field_info = {
                "name": name,
                "distinct": distinct,
                "null_count": null_count,
                "ratio": ratio,
                "is_unique": is_unique,
                "name_score": name_score
            }
            
            all_fields.append(field_info)
            
            if is_unique:
                unique_fields.append(field_info)
            elif ratio >= threshold:
                candidate_fields.append(field_info)
    
    # If we have unique fields, pick the best one
    if unique_fields:
        # Sort by name score (descending), then by name length (shorter preferred)
        unique_fields.sort(key=lambda x: (-x["name_score"], len(x["name"])))
        best = unique_fields[0]
        return {
            "field": best["name"],
            "is_unique": True,
            "uniqueness_ratio": best["ratio"],
            "reason": f"unique field with name_score={best['name_score']}"
        }
    
    # If no unique fields, pick best candidate above threshold
    if candidate_fields:
        # Sort by ratio (descending), then name score (descending)
        candidate_fields.sort(key=lambda x: (-x["ratio"], -x["name_score"]))
        best = candidate_fields[0]
        return {
            "field": best["name"],
            "is_unique": False,
            "uniqueness_ratio": best["ratio"],
            "reason": f"best candidate above threshold ({threshold})"
        }
    
    # No suitable field found - return info about best available
    if all_fields:
        # Sort by ratio descending, prefer no nulls, then by name score
        all_fields.sort(key=lambda x: (-x["ratio"], x["null_count"], -x["name_score"]))
        best = all_fields[0]
        return {
            "field": None, 
            "reason": f"no unique or high-cardinality fields above {threshold}",
            "best_below_threshold": {
                "field": best["name"],
                "ratio": best["ratio"],
                "null_count": best["null_count"]
            }
        }
    
    return {"field": None, "reason": f"no unique or high-cardinality fields above {threshold}"}


def process_analysis_file(filepath: Path, threshold: float) -> dict:
    """Process a single analysis JSON file."""
    with open(filepath) as f:
        data = json.load(f)
    
    result = {
        "file": str(filepath),
        "route": data.get("route", ""),
        "name": data.get("name", ""),
        "base_name": data.get("base_name", ""),
        "total_features": data.get("analysis", {}).get("total_features", 0)
    }
    
    if "error" in data:
        result["error"] = data["error"]
        result["picked_id"] = None
    elif "analysis" not in data:
        result["error"] = "no analysis"
        result["picked_id"] = None
    elif "error" in data["analysis"]:
        result["error"] = data["analysis"]["error"]
        result["picked_id"] = None
    else:
        pick = pick_best_id(data["analysis"], threshold)
        result["picked_id"] = pick
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Pick best unique ID field for each dataset"
    )
    parser.add_argument(
        "--input-dir", "-i",
        type=Path,
        default=Path("field_analysis"),
        help="Directory containing analysis JSON files"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output JSON file for results"
    )
    parser.add_argument(
        "--threshold", "-t",
        type=float,
        default=0.95,
        help="Minimum uniqueness ratio for non-unique field candidates (default: 0.95)"
    )
    parser.add_argument(
        "--update-routes",
        type=Path,
        default=None,
        help="Path to routes.json to update with promoteId field"
    )
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        print(f"Error: Input directory not found: {args.input_dir}")
        return 1
    
    # Find all analysis JSON files
    json_files = list(args.input_dir.rglob("*.json"))
    print(f"Found {len(json_files)} analysis files in {args.input_dir}")
    
    results = []
    stats = {
        "with_unique_id": 0,
        "with_candidate_id": 0,
        "no_id_found": 0,
        "errors": 0
    }
    
    for filepath in sorted(json_files):
        result = process_analysis_file(filepath, args.threshold)
        results.append(result)
        
        if "error" in result:
            stats["errors"] += 1
        elif result["picked_id"] is None or result["picked_id"].get("field") is None:
            stats["no_id_found"] += 1
        elif result["picked_id"].get("is_unique"):
            stats["with_unique_id"] += 1
        else:
            stats["with_candidate_id"] += 1
    
    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Datasets with unique ID field: {stats['with_unique_id']}")
    print(f"Datasets with candidate ID (>{args.threshold}): {stats['with_candidate_id']}")
    print(f"Datasets with no suitable ID: {stats['no_id_found']}")
    print(f"Datasets with errors: {stats['errors']}")
    
    # Print details for each category
    print(f"\n--- Datasets with unique ID ---")
    for r in results:
        if r.get("picked_id") and r["picked_id"].get("is_unique"):
            print(f"  {r['base_name']}: {r['picked_id']['field']}")
    
    print(f"\n--- Datasets with candidate ID ---")
    for r in results:
        if r.get("picked_id") and r["picked_id"].get("field") and not r["picked_id"].get("is_unique"):
            ratio = r["picked_id"].get("uniqueness_ratio", 0)
            print(f"  {r['base_name']}: {r['picked_id']['field']} ({ratio:.2%})")
    
    print(f"\n--- Datasets with no suitable ID ---")
    for r in results:
        if r.get("picked_id") is None or r["picked_id"].get("field") is None:
            if "error" not in r:
                picked = r.get("picked_id", {})
                best = picked.get("best_below_threshold")
                if best:
                    print(f"  {r['base_name']}: best={best['field']} ({best['ratio']:.2%}, nulls={best['null_count']})")
                else:
                    print(f"  {r['base_name']}: {picked.get('reason', 'unknown')}")
    
    print(f"\n--- Datasets with errors ---")
    for r in results:
        if "error" in r:
            print(f"  {r['base_name']}: {r['error']}")
    
    # Save results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump({"stats": stats, "results": results}, f, indent=2)
        print(f"\nResults saved to {args.output}")
    
    # Update routes.json with promoteId
    if args.update_routes:
        if not args.update_routes.exists():
            print(f"Error: Routes file not found: {args.update_routes}")
            return 1
        
        with open(args.update_routes) as f:
            routes = json.load(f)
        
        # Build lookup by route key
        route_to_id = {}
        for r in results:
            route = r.get("route")
            if route and r.get("picked_id") and r["picked_id"].get("field"):
                route_to_id[route] = r["picked_id"]["field"]
        
        # Update routes
        updated_count = 0
        for route_key, entry in routes.items():
            if route_key in route_to_id:
                entry["promoteid"] = route_to_id[route_key]
                updated_count += 1
        
        # Write back
        with open(args.update_routes, 'w') as f:
            json.dump(routes, f, indent=2)
        
        print(f"\nUpdated {updated_count} entries in {args.update_routes} with promoteId")
    
    return 0


if __name__ == "__main__":
    exit(main())
