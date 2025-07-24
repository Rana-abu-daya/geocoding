import re, asyncio, aiohttp
import tkinter as tk
from tkinter import ttk, scrolledtext

# --- Config & Helpers (your existing functions) ---
CENSUS_ONELINE = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
CENSUS_COORDS = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
GEOAPIFY_URL = "https://api.geoapify.com/v1/geocode/search"
LOCATIONIQ_URL = "https://us1.locationiq.com/v1/search.php"
GEOAPIFY_KEY = "688137e77320d260636013hie808391"
LOCATIONIQ_KEY = "pk.ced30e90f6fd2b6f4b45e9859d80be33"
TIMEOUT = aiohttp.ClientTimeout(total=10)

async def census_oneline(session, address):
    params = {"address": address, "benchmark": "Public_AR_Current", "vintage":"Current_Current", "format":"json", "layers":"all"}
    async with session.get(CENSUS_ONELINE, params=params, timeout=TIMEOUT) as r:
        if r.status != 200: return None, None
        js = await r.json()
        matches = js["result"]["addressMatches"]
        if not matches:
            return None, None
        m = matches[0]
        return m.get("geographies"), m.get("addressComponents", {}).get("city")

async def geoapify_forward(session, address):
    params = {"text": address, "apiKey": GEOAPIFY_KEY, "limit":1}
    async with session.get(GEOAPIFY_URL, params=params, timeout=TIMEOUT) as r:
        js = await r.json()
        feats = js.get("features", [])
        if feats:
            p = feats[0]["properties"]
            return p.get("lat"), p.get("lon")
    return None

async def locationiq_forward(session, address):
    await asyncio.sleep(0.5)
    params = {"key": LOCATIONIQ_KEY, "q": address, "format": "json"}
    async with session.get(LOCATIONIQ_URL, params=params, timeout=TIMEOUT) as r:
        if r.status != 200: return None
        js = await r.json()
        if isinstance(js, list) and js:
            return float(js[0]["lat"]), float(js[0]["lon"])
    return None

async def census_coords(session, lat, lon):
    params = {"x": lon, "y": lat, "benchmark":"Public_AR_Current", "vintage":"Current_Current", "format":"json", "layers":"all"}
    async with session.get(CENSUS_COORDS, params=params, timeout=TIMEOUT) as r:
        if r.status != 200: return None
        js = await r.json()
        geo = js.get("result", {}).get("geographies", {})
        wanted = ["Counties", "Incorporated Places", "119th Congressional Districts",
                  "2024 State Legislative Districts - Upper", "2024 State Legislative Districts - Lower",
                  "Unified School Districts", "Secondary School Districts", "Elementary School Districts"]
        if any(geo.get(layer) for layer in wanted):
            return geo
    return None

async def lookup_full(session, address):
    geo, city_fb = await census_oneline(session, address)
    src = "CensusOneline" if geo else None

    if not geo:
        coords = await geoapify_forward(session, address)
        src = "Geoapify" if coords else src
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

    city = city_fb or geo.get("Incorporated Places", [{}])[0].get("BASENAME", "").upper()
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

# --- GUI helpers ---
async def run_tk(root, interval=0.05):
    try:
        while True:
            root.update()
            await asyncio.sleep(interval)
    except tk.TclError:
        pass

def on_geocode():
    addr = addr_var.get().strip()
    if not addr:
        output.insert(tk.END, "⚠️ Enter an address first.\n")
    else:
        asyncio.create_task(do_geocode(addr))

async def do_geocode(address):
    output.insert(tk.END, f"🔍 Geocoding '{address}' …\n")
    import ssl, certifi

    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(limit=5, ssl=ssl_ctx)
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        result = await lookup_full(session, address)

        try:
            result = await lookup_full(session, address)
        except Exception as e:
            result = {"Input": address, "Error": f"Exception: {e}"}
    for key, val in result.items():
        output.insert(tk.END, f"{key}: {val}\n")
    output.insert(tk.END, "\n")  # Add space before next result
    output.see(tk.END)

def build_gui():
    root = tk.Tk()
    root.title("Single Address Geocoder")
    global addr_var, output
    addr_var = tk.StringVar()
    ttk.Label(root, text="Address:").grid(row=0, column=0, padx=5, pady=5)
    ttk.Entry(root, textvariable=addr_var, width=60).grid(row=0, column=1, padx=5, pady=5)
    ttk.Button(root, text="Geocode", command=on_geocode).grid(row=0, column=2, padx=5)
    output = scrolledtext.ScrolledText(root, width=100, height=20)
    output.grid(row=1, column=0, columnspan=3, padx=5, pady=5)
    return root

async def main():
    root = build_gui()
    await run_tk(root)

if __name__ == "__main__":
    asyncio.run(main())
