import ccxt
import os

kraken = ccxt.kraken({
    "apiKey": os.getenv("KRAKEN_API_KEY"),
    "secret": os.getenv("KRAKEN_SECRET"),
})

print(kraken.fetch_balance())
# OUT:
# ntrajic@DESKTOP-6PK7L32:/mnt/c/SRC/Py/backpack-basis-trading-monitor$ python find_kraken_balance.py 
# {'info': {'error': [], 'result': 
#           {'SOL': {'balance': '0.0000419352', 'hold_trade': '0.0000000000'}, 
#            'USDC': {'balance': '96.61497165', 'hold_trade': '0.00000000'}}}, 
#            'timestamp': None, 'datetime': None, 
#            'SOL': {'free': 4.19352e-05, 'used': 0.0, 'total': 4.19352e-05}, 
#            'USDC': {'free': 96.61497165, 'used': 0.0, 'total': 96.61497165}, 
#            'free': {'SOL': 4.19352e-05, 'USDC': 96.61497165}, 
#            'used': {'SOL': 0.0, 'USDC': 0.0}, 
#            'total': {'SOL': 4.19352e-05, 'USDC': 96.61497165}}