
import requests
import re

SEC_HEADERS = {"User-Agent": "Naveen Mokkapati navmok@gmail.com", "Accept": "*/*"}
SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)
RE_XML_HREF = re.compile(r'href="([^"]+\.xml)"', re.IGNORECASE)

def list_xmls():
    cik = "36644"
    acc = "000003664419000001"
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}"
    print(f"Fetching {url}")
    r = SESSION.get(url)
    
    xmls = RE_XML_HREF.findall(r.text)
    print("XMLs found:")
    for x in xmls:
        print(f" - {x}")

    # Check content of each
    for x in xmls:
        full_url = f"https://www.sec.gov{x}" if x.startswith('/') else (x if x.startswith('http') else f"{url}/{x}")
        print(f"\nChecking {full_url}...")
        r2 = SESSION.get(full_url)
        print(f"Size: {len(r2.content)}")
        if b'informationTable' in r2.content[:2000] or b'infoTable' in r2.content[:2000]:
            print("MATCHES info table check (first 2kb)")
        else:
            print("NO MATCH info table check (first 2kb)")
            
        if b'informationTable' in r2.content[:10000] or b'infoTable' in r2.content[:10000]:
            print("MATCHES info table check (first 10kb)")


if __name__ == "__main__":
    list_xmls()
