"""
Find 13f.info API endpoints

This script investigates how 13f.info loads holdings data.
"""
import requests
import re
import json

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

session = requests.Session()
session.headers.update({"User-Agent": UA})

# Test filing URL
FILING_URL = "https://13f.info/13f/000090445425000605-abdiel-capital-advisors-lp-q3-2025"
FILING_ID = "000090445425000605"

print("=" * 70)
print("Finding 13f.info API")
print("=" * 70)

# 1. Check the HTML for API endpoints or data URLs
print("\n1. Checking HTML for API clues...")
r = session.get(FILING_URL, timeout=30)
html = r.text

# Look for API URLs in the HTML
api_patterns = [
    r'https?://[^"\'>\s]*api[^"\'>\s]*',
    r'https?://[^"\'>\s]*\.json[^"\'>\s]*',
    r'/api/[^"\'>\s]*',
    r'fetch\(["\']([^"\']+)["\']',
    r'axios\.[a-z]+\(["\']([^"\']+)["\']',
    r'\.get\(["\']([^"\']+)["\']',
]

found_urls = set()
for pattern in api_patterns:
    matches = re.findall(pattern, html, re.IGNORECASE)
    for m in matches:
        if isinstance(m, tuple):
            m = m[0]
        found_urls.add(m)

if found_urls:
    print(f"   Found {len(found_urls)} potential API URLs:")
    for url in sorted(found_urls)[:20]:
        print(f"   - {url}")
else:
    print("   No obvious API URLs found in HTML")

# 2. Check for embedded JSON data
print("\n2. Checking for embedded JSON data...")
json_patterns = [
    r'window\.__DATA__\s*=\s*({.+?});',
    r'window\.initialData\s*=\s*({.+?});',
    r'<script[^>]*type="application/json"[^>]*>(.+?)</script>',
    r'data-holdings="([^"]+)"',
    r'data-props="([^"]+)"',
]

for pattern in json_patterns:
    matches = re.findall(pattern, html, re.DOTALL)
    if matches:
        print(f"   Found JSON with pattern: {pattern[:50]}...")
        for m in matches[:2]:
            print(f"   Preview: {m[:200]}...")

# 3. Try common API patterns
print("\n3. Trying common API endpoint patterns...")

api_attempts = [
    f"https://13f.info/api/filing/{FILING_ID}",
    f"https://13f.info/api/13f/{FILING_ID}",
    f"https://13f.info/api/holdings/{FILING_ID}",
    f"https://13f.info/13f/{FILING_ID}.json",
    f"https://13f.info/filing/{FILING_ID}.json",
    f"https://13f.info/api/v1/filing/{FILING_ID}",
    f"https://13f.info/api/v1/holdings/{FILING_ID}",
    "https://13f.info/api/filings",
    "https://13f.info/api/managers",
]

for url in api_attempts:
    try:
        r = session.get(url, timeout=10)
        content_type = r.headers.get('content-type', '')
        print(f"   {url}")
        print(f"      Status: {r.status_code}, Content-Type: {content_type[:50]}")
        if r.status_code == 200 and 'json' in content_type.lower():
            print(f"      ✅ FOUND JSON API!")
            try:
                data = r.json()
                print(f"      Keys: {list(data.keys()) if isinstance(data, dict) else f'Array of {len(data)} items'}")
            except:
                pass
        elif r.status_code == 200 and len(r.text) < 500:
            print(f"      Content: {r.text[:200]}")
    except Exception as e:
        print(f"   {url} -> Error: {e}")

# 4. Check for GraphQL
print("\n4. Checking for GraphQL endpoint...")
graphql_urls = [
    "https://13f.info/graphql",
    "https://13f.info/api/graphql",
]

for url in graphql_urls:
    try:
        # Try introspection query
        r = session.post(url, json={"query": "{ __schema { types { name } } }"}, timeout=10)
        print(f"   {url} -> Status: {r.status_code}")
        if r.status_code == 200:
            print(f"      ✅ GraphQL endpoint found!")
            print(f"      Response: {r.text[:300]}")
    except Exception as e:
        print(f"   {url} -> Error: {e}")

# 5. Look for JavaScript files that might contain API URLs
print("\n5. Checking JavaScript files for API URLs...")
js_pattern = r'<script[^>]+src="([^"]+\.js[^"]*)"'
js_files = re.findall(js_pattern, html)
print(f"   Found {len(js_files)} JS files")

for js_url in js_files[:5]:
    if not js_url.startswith('http'):
        js_url = f"https://13f.info{js_url}" if js_url.startswith('/') else f"https://13f.info/{js_url}"
    
    print(f"\n   Checking: {js_url}")
    try:
        r = session.get(js_url, timeout=15)
        if r.status_code == 200:
            js_content = r.text
            # Look for API patterns in JS
            api_in_js = re.findall(r'["\']/(api|graphql|v1)/[^"\']+["\']', js_content)
            fetch_in_js = re.findall(r'fetch\s*\(\s*["\']([^"\']+)["\']', js_content)
            
            if api_in_js:
                print(f"      Found API patterns: {api_in_js[:5]}")
            if fetch_in_js:
                print(f"      Found fetch URLs: {fetch_in_js[:5]}")
            
            # Look for the data loading function
            if 'holdings' in js_content.lower():
                # Find context around 'holdings'
                idx = js_content.lower().find('holdings')
                context = js_content[max(0, idx-100):idx+200]
                print(f"      Holdings context: ...{context}...")
    except Exception as e:
        print(f"      Error: {e}")

# 6. Check page source for React/data hydration
print("\n6. Checking for server-side rendered data...")
# Sometimes data is in a script tag for hydration
script_tags = re.findall(r'<script[^>]*>([^<]{100,})</script>', html, re.DOTALL)
for i, script in enumerate(script_tags[:5]):
    if any(keyword in script.lower() for keyword in ['holdings', 'filing', 'cusip', 'shares']):
        print(f"   Script {i+1} contains relevant keywords:")
        print(f"   Preview: {script[:500]}...")

print("\n" + "=" * 70)
print("Done investigating")
print("=" * 70)