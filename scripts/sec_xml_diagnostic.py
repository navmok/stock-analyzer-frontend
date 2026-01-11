"""
Diagnostic tool to examine actual SEC XML structure
This will help us understand why shares data isn't being parsed
"""
import requests
from lxml import etree
import io

SEC_HEADERS = {
    "User-Agent": "Naveen Mokkapati navmok@gmail.com",
    "Accept": "*/*",
}

# Recent filings we know exist
TEST_CASES = [
    {
        "name": "ARK Latest",
        "cik": "0001649339",
    },
    {
        "name": "Beach Point Latest",
        "cik": "0001453885",
    },
    {
        "name": "Jane Street Latest",
        "cik": "0001595888",
    }
]

def build_infotable_url(cik, accession):
    cik_num = str(int(cik))  # removes leading zeros for the /data/ path
    accession_nodash = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{accession_nodash}/infotable.xml"

def get_latest_13f_accession(cik):
    cik10 = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    recent = data["filings"]["recent"]
    for form, acc in zip(recent["form"], recent["accessionNumber"]):
        if form in ("13F-HR", "13F-HR/A"):
            return acc  # real accession with dashes

    return None

def find_infotable_url(cik, accession):
    cik_num = str(int(cik))
    acc_nodash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_nodash}"
    index_url = f"{base}/index.json"

    r = requests.get(index_url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    items = r.json()["directory"]["item"]

    # Build a simple name list
    names = [it.get("name", "") for it in items]
    names_l = [n.lower() for n in names]

    # 1) Prefer common infotable XML names
    preferred = [
        "infotable.xml",
        "informationtable.xml",
        "infotable1.xml",
        "informationtable1.xml",
        "form13finfotable.xml",
        "form13finformationtable.xml",
    ]
    for p in preferred:
        if p in names_l:
            real_name = names[names_l.index(p)]
            return f"{base}/{real_name}"

    # 2) Fallback: any XML containing "info" + "table"
    for n in names:
        nl = n.lower()
        if nl.endswith(".xml") and ("info" in nl) and ("table" in nl):
            return f"{base}/{n}"

    # 3) Fallback: ANY xml in the folder (some filers use odd names)
    for n in names:
        if n.lower().endswith(".xml"):
            return f"{base}/{n}"

    # 4) Fallback: some filers provide infotable in a .txt file
    for n in names:
        nl = n.lower()
        if nl.endswith(".txt") and ("info" in nl or "table" in nl or "13f" in nl):
            return f"{base}/{n}"

    # If we got here: nothing usable. Print the folder contents for debugging.
    print("🔎 No xml/txt candidate found. Folder contents:")
    for n in names[:60]:
        print(f"  - {n}")
    return None

def fetch_and_analyze_xml(url):
    """Fetch XML and show its structure"""
    print(f"\n{'='*80}")
    print(f"Fetching: {url}")
    print(f"{'='*80}")
    
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=30)
        
        if r.status_code == 404:
            print("❌ File not found (404)")
            
            # Try alternative filename
            alt_url = url.replace("infotable.xml", "informationtable.xml")
            print(f"Trying alternative: {alt_url}")
            r = requests.get(alt_url, headers=SEC_HEADERS, timeout=30)
            
            if r.status_code == 404:
                print("❌ Alternative also not found")
                return None
        
        r.raise_for_status()
        xml_content = r.content
        
        print(f"✓ Downloaded {len(xml_content)} bytes")
        print(f"\n--- First 2000 characters of XML ---")
        print(xml_content[:2000].decode('utf-8', errors='replace'))
        print(f"--- End preview ---\n")
        
        # Parse and analyze structure
        try:
            # Some filings return .txt that contains XML inside it
            content = xml_content

            if not content.lstrip().startswith(b"<"):
                # try to extract the first XML tag
                start = content.find(b"<informationTable")
                if start == -1:
                    start = content.find(b"<ns1:informationTable")
                if start != -1:
                    content = content[start:]

            root = etree.fromstring(content)
            
            # Find first infoTable element
            print("\n🔍 Searching for infoTable elements...")
            
            # Try with and without namespace
            info_tables = root.xpath('.//*[local-name()="infoTable"]')
            
            if not info_tables:
                print("❌ No infoTable elements found!")
                print("\nAvailable root-level tags:")
                for child in root[:5]:
                    tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                    print(f"  - {tag}")
                return None
            
            print(f"✓ Found {len(info_tables)} infoTable elements")
            
            # Analyze first holding
            first_info = info_tables[0]
            print(f"\n📋 First holding structure:")
            
            for child in first_info:
                tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                text = child.text.strip() if child.text else "(empty)"
                
                # For nested elements like votingAuthority
                if len(child) > 0:
                    print(f"  {tag}:")
                    for subchild in child:
                        subtag = subchild.tag.split('}')[-1] if '}' in subchild.tag else subchild.tag
                        subtext = subchild.text.strip() if subchild.text else "(empty)"
                        print(f"    {subtag}: {subtext}")
                else:
                    print(f"  {tag}: {text}")
            
            # Check for shares specifically
            print(f"\n🔎 Looking for shares field (sshPrnamt)...")
            shares_nodes = first_info.xpath('.//*[local-name()="sshPrnamt"]')
            shares_elem = shares_nodes[0] if shares_nodes else None
            if shares_elem is not None:
                print(f"✓ FOUND: sshPrnamt = '{shares_elem.text}'")
            else:
                print("❌ NOT FOUND: sshPrnamt element missing")
                
            # Check for share type
            print(f"🔎 Looking for share type field (sshPrnamtType)...")
            type_nodes = first_info.xpath('.//*[local-name()="sshPrnamtType"]')
            type_elem = type_nodes[0] if type_nodes else None
            if type_elem is not None:
                print(f"✓ FOUND: sshPrnamtType = '{type_elem.text}'")
            else:
                print("❌ NOT FOUND: sshPrnamtType element missing")
            
            return True
            
        except Exception as e:
            print(f"❌ XML parsing error: {e}")
            return None
            
    except Exception as e:
        print(f"❌ Request error: {e}")
        return None


def main():
    print("=" * 80)
    print("SEC 13F XML Structure Diagnostic Tool")
    print("=" * 80)
    
    for test_case in TEST_CASES:
        print(f"\n\n{'#'*80}")
        print(f"# {test_case['name']}")
        print(f"# CIK: {test_case['cik']}")
        print(f"{'#'*80}")
        
        acc = get_latest_13f_accession(test_case["cik"])
        if not acc:
            print("❌ No 13F filing found")
            continue

        url = find_infotable_url(test_case["cik"], acc)
        if not url:
            print(f"❌ 13F found ({acc}) but no infotable XML")
            continue

        fetch_and_analyze_xml(url)
    
    print("\n\n" + "="*80)
    print("DIAGNOSTIC COMPLETE")
    print("="*80)
    print("\nNext steps:")
    print("1. If sshPrnamt is found: XML parsing logic needs fixing")
    print("2. If sshPrnamt is NOT found: These managers don't report shares")
    print("3. If 404 errors: File structure/naming is different")


if __name__ == "__main__":
    main()