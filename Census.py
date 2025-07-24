import re, asyncio, aiohttp
import pandas as pd

# --- Config ---
CENSUS_ONELINE = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
CENSUS_COORDS = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
GEOAPIFY_URL = "https://api.geoapify.com/v1/geocode/search"
LOCATIONIQ_URL = "https://us1.locationiq.com/v1/search.php"
GEOAPIFY_KEY = "688137e77320d260636013hie808391"
LOCATIONIQ_KEY = "pk.ced30e90f6fd2b6f4b45e9859d80be33"
CONCURRENT = 5
TIMEOUT = aiohttp.ClientTimeout(total=10)

# --- Helpers ---

# Census one-line lookup with city fallback
async def census_oneline(session, address):
    params = {
        "address": (address),
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
        "layers": "all"
    }
    async with session.get(CENSUS_ONELINE, params=params, timeout=TIMEOUT) as r:

        if r.status != 200:
            return None, None
        js = await r.json()
        matches = js["result"]["addressMatches"]
        if not matches:
            return None, None
        match = matches[0]
        geo = match.get("geographies")
        city_fb = match.get("addressComponents", {}).get("city")  # user-friendly city fallback
        return geo, city_fb

# Geoapify forward geocode
async def geoapify_forward(session, address):
    params = {"text": (address), "apiKey": GEOAPIFY_KEY, "limit": 1}
    async with session.get(GEOAPIFY_URL, params=params, timeout=TIMEOUT) as r:
        js = await r.json()
        feats = js.get("features", [])
        if feats:
            props = feats[0]["properties"]
            return props.get("lat"), props.get("lon")
    return None

# LocationIQ forward geocode
async def locationiq_forward(session, address):
    await asyncio.sleep(0.5)
    params = {"key": LOCATIONIQ_KEY, "q": (address), "format": "json"}
    async with session.get(LOCATIONIQ_URL, params=params, timeout=TIMEOUT) as r:
        if r.status != 200:
            return None
        js = await r.json()
        if isinstance(js, list) and js:
            return float(js[0]["lat"]), float(js[0]["lon"])
    return None

# Census reverse geocode by coordinates
async def census_coords(session, lat, lon):
    params = {
        "x": lon, "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
        "layers": "all"
    }
    async with session.get(CENSUS_COORDS, params=params, timeout=TIMEOUT) as r:
        if r.status != 200:
            return None
        js = await r.json()
        print(r)
        geo = js.get("result", {}).get("geographies", {})
        wanted = [
            "Counties", "Incorporated Places",
            "119th Congressional Districts",
            "2024 State Legislative Districts - Upper",
            "2024 State Legislative Districts - Lower",
            "Unified School Districts",
            "Secondary School Districts",
            "Elementary School Districts"
        ]
        if any(geo.get(layer) for layer in wanted):
            return geo
    return None

# Full geocoding workflow
async def lookup_full(session, address):
    geo, city_fb = await census_oneline(session, address)
    src = "CensusOneline" if geo else None

    if not geo:
        coords = await geoapify_forward(session, address)
        src = "Geoapify" if coords else None
        if coords:
            geo = await census_coords(session, *coords)
            src = f"{src}+CensusCoords" if geo else f"{src}(noCensus)"

    if not geo:
        coords = await locationiq_forward(session, address)
        src = "LocationIQ" if coords else src
        if coords:
            geo = await census_coords(session, *coords)
            src = f"{src}+CensusCoords" if geo else f"{src}(noCensus)"

    if not geo:
        return {"Input": address, "Error": src or "No match"}

    city =   city_fb or geo.get("Incorporated Places", [{}])[0].get("BASENAME").upper() or ""
    return {
        "Input": address,
        "County": geo["Counties"][0]["NAME"],
        "City": city,
        "Congressional": geo["119th Congressional Districts"][0]["NAME"],
        "StateSenate": geo["2024 State Legislative Districts - Upper"][0]["NAME"],
        "StateHouse": geo["2024 State Legislative Districts - Lower"][0]["NAME"],
        "UnifiedSD": geo.get("Unified School Districts", [{}])[0].get("NAME", ""),
        "SecondarySD": geo.get("Secondary School Districts", [{}])[0].get("NAME", ""),
        "ElementarySD": geo.get("Elementary School Districts", [{}])[0].get("NAME", ""),
        "Source": src
    }

# Batch processing
async def process_csv(path):
    df = pd.read_csv(path, dtype=str)
    addrs = df['Address'].tolist()
    conn = aiohttp.TCPConnector(limit=CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(connector=conn, timeout=TIMEOUT) as session:
        results = await asyncio.gather(*[lookup_full(session, addr) for addr in addrs])
    return df, results

# Entry point
if __name__ == "__main__":
    orig, res = asyncio.run(process_csv("voter_addresses.csv"))
    out = pd.DataFrame(res)
    final = pd.concat([orig.reset_index(drop=True), out], axis=1)
    final.to_csv("voter_geocoded_fulllast.csv", index=False)
    print(final.Source.value_counts())
