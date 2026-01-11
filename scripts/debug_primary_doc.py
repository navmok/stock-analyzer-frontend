
import requests
from lxml import etree
import io

SEC_HEADERS = {"User-Agent": "Naveen Mokkapati navmok@gmail.com", "Accept": "*/*"}
SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)

def check_primary_doc():
    url = "https://www.sec.gov/Archives/edgar/data/36644/000003664419000001/primary_doc.xml"
    print(f"Checking {url}...")
    r = SESSION.get(url)
    
    # Try parsing it as if it were an info table
    holdings = []
    try:
        for _, elem in etree.iterparse(io.BytesIO(r.content), events=("end",), recover=True):
            if not isinstance(elem.tag, str): continue
            tag = elem.tag.rsplit('}', 1)[-1].lower()
            
            # The parser in scrape_single_13f.py looks for ANY tag named 'infotable'
            if tag == "infotable":
                 holdings.append("row")
        
        print(f"Count of 'infotable' tags in primary_doc: {len(holdings)}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_primary_doc()
