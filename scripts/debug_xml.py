import requests
from lxml import etree

SEC_HEADERS = {
    "User-Agent": "Naveen Mokkapati navmok@gmail.com",
    "Accept": "application/xml,text/xml,*/*;q=0.9",
}

# Test URL from the output
xml_url = "https://www.sec.gov/Archives/edgar/data/1453885/000145388525000002/primary_doc.xml"

r = requests.get(xml_url, headers=SEC_HEADERS, timeout=30)
r.raise_for_status()

parser = etree.XMLParser(remove_blank_text=True)
root = etree.fromstring(r.content, parser)

# Find all unique tag names
tags = set()
for el in root.iter():
    # Extract local name without namespace
    tag = el.tag
    if '}' in tag:
        tag = tag.split('}', 1)[1]
    tags.add(tag)

print("All tags found in XML:")
for tag in sorted(tags):
    print(f"  {tag}")

# Look for tags containing "value" or "total"
print("\nTags containing 'value' or 'total':")
for tag in sorted(tags):
    if 'value' in tag.lower() or 'total' in tag.lower():
        print(f"  {tag}")

# Print first 20 elements and their text
print("\nFirst elements and their text content:")
count = 0
for el in root.iter():
    count += 1
    if count > 30:
        break
    tag = el.tag
    if '}' in tag:
        tag = tag.split('}', 1)[1]
    text = el.text.strip() if el.text else ""
    if text:
        print(f"  {tag}: {text[:100]}")
