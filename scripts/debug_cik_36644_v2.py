
import requests
import io
from lxml import etree

SEC_HEADERS = {"User-Agent": "Naveen Mokkapati navmok@gmail.com", "Accept": "*/*"}
SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)

def debug_filing_urls_and_parsing():
    cik = "36644"
    acc = "000003664419000001"
    directory_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}"
    print(f"Checking directory: {directory_url}")
    
    # 1. Manually specify the known non-standard XML name
    xml_url = f"{directory_url}/PTR289_01072019.xml"
    print(f"Targeting specific XML: {xml_url}")
    
    r = SESSION.get(xml_url)
    print(f"Status: {r.status_code}")
    print(f"Content Length: {len(r.content)}")
    print(f"First 500 bytes:\n{r.content[:500]}")
    
    # 2. Check detection logic (first 2kb)
    head = r.content[:2000]
    if b'informationTable' in head or b'infoTable' in head:
        print("✅ Detection logic: MATCHED (informationTable tag found)")
    else:
        print("❌ Detection logic: FAILED (tag not found in first 2KB)")
        # Show where it actually is
        idx1 = r.content.find(b'informationTable')
        idx2 = r.content.find(b'infoTable')
        print(f"Actual location of 'informationTable': {idx1}")
        print(f"Actual location of 'infoTable': {idx2}")

    # 3. Parse and count
    print("\nParsing holdings...")
    holdings = []
    try:
        # Replicating the namespace-agnostic logic from scrape_single_13f.py
        for _, elem in etree.iterparse(io.BytesIO(r.content), events=("end",), recover=True):
            if not isinstance(elem.tag, str): continue
            tag = elem.tag.rsplit('}', 1)[-1].lower()
            
            if tag == "infotable":
                # Extract Issuer Name to verify
                name = "Unknown"
                for child in elem:
                    if child.tag.rsplit('}', 1)[-1].lower() == 'nameofissuer':
                        name = child.text
                holdings.append(name)
                elem.clear()
        
        print(f"parsed {len(holdings)} holdings.")
        if len(holdings) > 0:
            print("First 5:", holdings[:5])
            print("Last 5:", holdings[-5:])
            
    except Exception as e:
        print(f"Parse error: {e}")

if __name__ == "__main__":
    debug_filing_urls_and_parsing()
