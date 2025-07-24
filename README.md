It follows a three-tiered fallback strategy:
1- Primary (Census Geocoder API – One-line lookup):Attempts to directly get geography layers from the U.S. Census Bureau using a cleaned full address.
2- Fallback 1 (Geoapify → Coordinates(Lat & long) → Census Reverse): If the first step fails, it geocodes the address to (lat, lon) using Geoapify, then sends that to the Census Reverse Geocoder to extract geographic boundaries.
3- Fallback 2 (LocationIQ → Coordinates(Lat & long) → Census Reverse): If Geoapify also fails, it tries LocationIQ as an alternative forward geocoder, followed by another Census reverse lookup.


the output result looks like this

https://docs.google.com/spreadsheets/d/1gRql8RIzUHLHUD1Zx76_Xof--nJ95OIlUJkUxhQ8PZI/edit?usp=sharing
