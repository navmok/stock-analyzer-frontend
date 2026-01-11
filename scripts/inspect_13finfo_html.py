"""
Inspect the saved 13f.info HTML file
"""
import re

print("Reading 13finfo_filing_page.html...")

with open("13finfo_filing_page.html", "r", encoding="utf-8") as f:
    html = f.read()

print(f"File size: {len(html)} characters")

# Find ALL script tag contents
print("\n" + "=" * 70)
print("ALL SCRIPT TAG CONTENTS")
print("=" * 70)

script_pattern = r'<script[^>]*>([\s\S]*?)</script>'
scripts = re.findall(script_pattern, html)

for i, script in enumerate(scripts):
    script = script.strip()
    if script:
        print(f"\n--- Script {i+1} ({len(script)} chars) ---")
        print(script[:2000])
        if len(script) > 2000:
            print(f"... [{len(script) - 2000} more chars]")

# Look for the table structure
print("\n" + "=" * 70)
print("TABLE STRUCTURE")
print("=" * 70)

table_pattern = r'<table[^>]*>([\s\S]*?)</table>'
tables = re.findall(table_pattern, html)
print(f"Found {len(tables)} tables")

for i, table in enumerate(tables):
    print(f"\n--- Table {i+1} ({len(table)} chars) ---")
    print(table[:3000])

# Look for any data- attributes
print("\n" + "=" * 70)
print("DATA ATTRIBUTES")
print("=" * 70)

data_attrs = re.findall(r'data-[a-z-]+="([^"]*)"', html)
print(f"Found {len(data_attrs)} data attributes")
unique_attrs = set(data_attrs)
for attr in sorted(unique_attrs)[:30]:
    if len(attr) > 10:  # Only show meaningful ones
        print(f"  {attr[:100]}")

# Look for turbo-frame elements (Rails Turbo)
print("\n" + "=" * 70)
print("TURBO FRAMES")
print("=" * 70)

turbo_pattern = r'<turbo-frame[^>]*>([\s\S]*?)</turbo-frame>'
frames = re.findall(turbo_pattern, html)
print(f"Found {len(frames)} turbo-frame elements")

for i, frame in enumerate(frames):
    print(f"\n--- Frame {i+1} ---")
    print(frame[:1000])

# Check for any JSON-like structures anywhere
print("\n" + "=" * 70)
print("JSON-LIKE STRUCTURES")
print("=" * 70)

# Look for arrays with objects
json_array_pattern = r'\[\s*\{\s*"[^"]+"\s*:'
matches = list(re.finditer(json_array_pattern, html))
print(f"Found {len(matches)} potential JSON arrays")

for m in matches[:5]:
    start = m.start()
    print(f"\n  At position {start}:")
    print(f"  Context: ...{html[max(0,start-50):start+200]}...")