import json, pprint
with open("products_full.json","r",encoding="utf-8") as f:
    data = json.load(f)
print("TOP KEYS:", list(data.keys()) if isinstance(data, dict) else type(data))
pp = pprint.PrettyPrinter(depth=2, width=100)
pp.pprint(data.get("data", data) if isinstance(data, dict) else (data[:1] if isinstance(data, list) else data))
