"""
Extract holdings data from 13f.info using embedded JavaScript data

The page loads data into window.holdingsChartData variable.
"""
import requests
import re
import json

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

session = requests.Session()
session.headers.update({"User-Agent": UA})

# Test filing URL
FILING_URL = "https://13f.info/13f/000090445425000605-abdiel-capital-advisors-lp-q3-2025"

print("=" * 70)
print("Extracting 13f.info Holdings Data")
print("=" * 70)

print(f"\nFetching: {FILING_URL}")
r = session.get(FILING_URL, timeout=30)
print(f"Status: {r.status_code}")

html = r.text

# Look for holdingsChartData or similar data variables
print("\n1. Looking for embedded data variables...")

# Common patterns for embedded data
patterns = [
    (r'window\.holdingsChartData\s*=\s*(\[.+?\]);', 'holdingsChartData'),
    (r'window\.cusipChartData\s*=\s*({.+?});', 'cusipChartData'),
    (r'holdingsChartData\s*=\s*(\[.+?\]);', 'holdingsChartData'),
    (r'var\s+holdings\s*=\s*(\[.+?\]);', 'holdings'),
    (r'"holdings"\s*:\s*(\[.+?\])', 'holdings JSON'),
    (r'data-holdings=["\']([^"\']+)["\']', 'data-holdings attr'),
]

for pattern, name in patterns:
    matches = re.findall(pattern, html, re.DOTALL)
    if matches:
        print(f"\n   ✅ Found {name}!")
        for m in matches[:1]:
            print(f"   Preview: {m[:500]}...")
            try:
                # Try to parse as JSON
                data = json.loads(m)
                print(f"   Parsed successfully! Type: {type(data)}, Length: {len(data) if isinstance(data, list) else 'N/A'}")
                if isinstance(data, list) and len(data) > 0:
                    print(f"   First item: {data[0]}")
            except json.JSONDecodeError as e:
                print(f"   JSON parse error: {e}")

# Look for script tags with data
print("\n2. Searching all script tags for data...")
script_pattern = r'<script[^>]*>(.*?)</script>'
scripts = re.findall(script_pattern, html, re.DOTALL)
print(f"   Found {len(scripts)} script tags")

for i, script in enumerate(scripts):
    # Skip external scripts and very short ones
    if len(script.strip()) < 50:
        continue
    
    # Look for data assignments
    if any(kw in script.lower() for kw in ['holdings', 'chartdata', 'cusip', 'value']):
        print(f"\n   Script {i+1} (length: {len(script)}):")
        
        # Try to find variable assignments
        var_patterns = [
            r'(window\.[a-zA-Z]+)\s*=\s*(\[[\s\S]*?\]);',
            r'(var\s+[a-zA-Z]+)\s*=\s*(\[[\s\S]*?\]);',
            r'(const\s+[a-zA-Z]+)\s*=\s*(\[[\s\S]*?\]);',
            r'(let\s+[a-zA-Z]+)\s*=\s*(\[[\s\S]*?\]);',
        ]
        
        for vp in var_patterns:
            var_matches = re.findall(vp, script)
            for var_name, var_value in var_matches:
                if 'chart' in var_name.lower() or 'data' in var_name.lower() or 'holding' in var_name.lower():
                    print(f"   Found: {var_name} = ...")
                    try:
                        data = json.loads(var_value)
                        print(f"   ✅ Parsed! Type: {type(data)}, Items: {len(data) if isinstance(data, list) else 'N/A'}")
                        if isinstance(data, list) and len(data) > 0:
                            print(f"   Sample: {json.dumps(data[0], indent=2)[:500]}")
                    except:
                        print(f"   Preview: {var_value[:300]}...")

# Look for turbo/stimulus data attributes (Rails)
print("\n3. Checking for Rails Turbo/Stimulus data...")
turbo_patterns = [
    r'data-controller="([^"]+)"',
    r'data-[a-z-]+-value="([^"]+)"',
    r'data-holdings-target="([^"]+)"',
]

for pattern in turbo_patterns:
    matches = re.findall(pattern, html)
    if matches:
        print(f"   Found: {pattern[:30]}... -> {matches[:3]}")

# Try finding the table data directly from the page
print("\n4. Looking for table data in different format...")

# The table might have data in tbody that gets populated
tbody_pattern = r'<tbody[^>]*>(.*?)</tbody>'
tbody_matches = re.findall(tbody_pattern, html, re.DOTALL)
print(f"   Found {len(tbody_matches)} tbody elements")

for i, tbody in enumerate(tbody_matches):
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL)
    print(f"   tbody {i+1}: {len(rows)} rows")
    if rows and len(rows) > 0:
        # Show first row
        cells = re.findall(r'<td[^>]*>(.*?)</td>', rows[0], re.DOTALL)
        cells_text = [re.sub(r'<[^>]+>', '', c).strip()[:20] for c in cells]
        print(f"   First row cells: {cells_text}")

# Check for async data loading patterns
print("\n5. Looking for data loading URL patterns...")
fetch_patterns = [
    r'fetch\(["\']([^"\']+)["\']',
    r'\.load\(["\']([^"\']+)["\']',
    r'url:\s*["\']([^"\']+)["\']',
    r'href="([^"]+\.json[^"]*)"',
]

for pattern in fetch_patterns:
    matches = re.findall(pattern, html)
    if matches:
        print(f"   Found fetch pattern: {matches[:5]}")

print("\n" + "=" * 70)
print("Investigation complete")
print("=" * 70)

# Save full HTML for manual inspection
with open("13finfo_filing_page.html", "w", encoding="utf-8") as f:
    f.write(html)
print(f"\nSaved full HTML to 13finfo_filing_page.html for manual inspection")