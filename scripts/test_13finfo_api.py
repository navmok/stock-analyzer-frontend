"""
Test 13f.info /data/ API endpoint
"""
import requests
import json

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

session = requests.Session()
session.headers.update({"User-Agent": UA})

# The API endpoint we found
FILING_ID = "000090445425000605"
DATA_URL = f"https://13f.info/data/13f/{FILING_ID}"

print("=" * 70)
print("Testing 13f.info Data API")
print("=" * 70)

print(f"\nFetching: {DATA_URL}")
r = session.get(DATA_URL, timeout=30)

print(f"Status: {r.status_code}")
print(f"Content-Type: {r.headers.get('content-type', 'N/A')}")
print(f"Content-Length: {len(r.text)} chars")

if r.status_code == 200:
    print("\n✅ SUCCESS!")
    
    # Try to parse as JSON
    try:
        data = r.json()
        print(f"Data type: {type(data)}")
        
        if isinstance(data, list):
            print(f"Number of holdings: {len(data)}")
            if len(data) > 0:
                print(f"\nFirst holding:")
                print(json.dumps(data[0], indent=2))
                
                print(f"\nAll keys in first holding: {list(data[0].keys())}")
                
                # Show a few more
                print(f"\nFirst 5 holdings summary:")
                for i, h in enumerate(data[:5]):
                    issuer = h.get('issuer_name', h.get('issuer', 'N/A'))[:30]
                    value = h.get('value', h.get('value_000', 'N/A'))
                    shares = h.get('shares', 'N/A')
                    print(f"  {i+1}. {issuer} - Value: {value}, Shares: {shares}")
        
        elif isinstance(data, dict):
            print(f"Keys: {list(data.keys())}")
            print(json.dumps(data, indent=2)[:2000])
            
    except json.JSONDecodeError:
        print("Not JSON, showing raw content:")
        print(r.text[:2000])
else:
    print(f"\n❌ Failed")
    print(f"Response: {r.text[:500]}")

# Also test the autocomplete endpoint
print("\n" + "=" * 70)
print("Testing autocomplete endpoint")
print("=" * 70)

autocomplete_url = "https://13f.info/data/autocomplete"
print(f"\nFetching: {autocomplete_url}")
r2 = session.get(autocomplete_url, timeout=30)
print(f"Status: {r2.status_code}")
print(f"Content-Type: {r2.headers.get('content-type', 'N/A')}")

if r2.status_code == 200:
    try:
        data = r2.json()
        print(f"Type: {type(data)}, Length: {len(data) if isinstance(data, list) else 'N/A'}")
        if isinstance(data, list) and len(data) > 0:
            print(f"Sample: {data[:3]}")
    except:
        print(f"Preview: {r2.text[:500]}")