"""
1. interactive monitor in the terminal
ntrajic@DESKTOP-6PK7L32:/mnt/c/SRC/Py/backpack-basis-trading-monitor
$ cdcx tui

2. data about the ticker SOL_USD
ntrajic@DESKTOP-6PK7L32:/mnt/c/SRC/Py/backpack-basis-trading-monitor>
$ cdcx market ticker SOL_USD
OUT:
{
"data": {
    "data": [
    {
        "a": "84.94",
        "b": "84.93",
        "c": "-0.0114",
        "h": "86.96",
        "i": "SOL_USD",
        "k": "84.94",
        "l": "83.58",
        "oi": "0",
        "t": 1779682690187,
        "v": "241483.744",
        "vv": "20655921.96"
    }
    ]
},
"status": "ok"
}
Based on standard exchange ticker field conventions:

a — ask price: lowest price a seller will accept → 84.94

b — bid price: highest price a buyer will pay → 84.93

c — change: 24h price change (absolute or %) → -0.0114 (SOL is down slightly)

h — high: 24h highest traded price → 86.96

i — instrument / symbol identifier → SOL_USD

k — likely ask again or last close — mirrors a here (84.94),
    possibly the best ask snapshot at a different moment

l — low: 24h lowest traded price → 83.58

oi — open interest: relevant for futures/perps, 0 here since this is spot

t — timestamp: Unix milliseconds → 1779682690187 = 2026-05-25 ~03:xx UTC

v — volume: 24h base asset volume in SOL → 241,483.744 SOL

vv — value volume: 24h volume in USD terms → $c.96

The bid/ask spread is razor thin: 84.94 - 84.93 = $0.01,
which is typical for a liquid SOL/USD pair and consistent with what you saw
in the triangular arbitrage scans — very little room for mispricing.

3. cdcx mcp : start your mcp server for your AI agent:
ntrajic@DESKTOP-6PK7L32:/mnt/c/SRC/Py/backpack-basis-trading-monitor
$ cdcx mcp
OUT:
cdcx MCP server starting...
services:  market
tools:     9
auth:      public-only
transport: stdio
Server ready.

4. view real-time ticker RWA prices
cdcx market ticker SPYUSD-PERP
ntrajic@DESKTOP-6PK7L32:/mnt/c/SRC/Py/backpack-basis-trading-monitor
$ cdcx market ticker SPYUSD-PERP
{
"data": {
    "data": [
    {
        "a": "751.84",
        "b": "751.84",
        "c": "-0.0011",
        "h": "755.64",
        "i": "SPYUSD-PERP",
        "k": "751.85",
        "l": "748.20",
        "oi": "484.325",
        "t": 1779683904785,
        "v": "609.014",
        "vv": "458560.31"
    }
    ]
},
"status": "ok"
}

5. check your account balances:
cdcx account summary

6. kraken REST API https://docs.kraken.com/api/

ntrajic@DESKTOP-6PK7L32:/mnt/c/SRC/Py/backpack-basis-trading-monitor
$ curl -s https://api.kraken.com/0/public/Ticker?pair=SOLUSDC | python3 -m json.tool
{
    "error": [],
    "result": {
        "SOLUSDC": {
            "a": [
                "85.740000",
                "81",
                "81.000"
            ],
            "b": [
                "85.720000",
                "7",
                "7.000"
            ],
            "c": [
                "85.770000",
                "15.39455657"
            ],
            "v": [
                "572.83152009",
                "10110.65182708"
            ],
            "p": [
                "85.235012",
                "85.390102"
            ],
            "t": [
                261,
                2046
            ],
            "l": [
                "84.760000",
                "83.570000"
            ],
            "h": [
                "85.810000",
                "86.860000"
            ],
            "o": "85.200000"
        }
    }
}
"""
