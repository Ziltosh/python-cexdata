# python-cexdata
Permet de récupérer sous forme de CSV l'historique des cours sur FTX et Binance.

Si des données partielles existent déjà, il ne récupère que ce qui manque (utile sur les timeframes les plus petits).

# Update
[CryptoRobot](https://github.com/CryptoRobotFr/ "CryptoRobotFR") a repris le code et à apporter des améliorations que vous pouvez retrouver dans la classe ExchangeDataManager du fichier exchange_data_manager.py

Avec aussi la possibilité d'explorer les données, un grand merci à lui.

## Utilisation

```python
from exchange_data_manager import ExchangeDataManager

exchange = ExchangeDataManager(exchange_name="binance", path_download="./database")

await exchange.download_data(coins=["BTC/USDT", "ETH/USDT", "XRP/USDT"], intervals=["1h", "1d"])

exchange.load_data(coin="BTC/USDT", interval="1h")

exchange.explore_data()
```

# Ancienne version
## Utilisation
Voir le fichier cexdata.ipynb
```python
from cexdata import CexData

cex = CexData("binance")

await cex.download_data(coins=["BTC-USD", "ETH-USD"], intervals=["1d", "1h"], end_date="2022-01-01 00:00:00")

cex = CexData("ftx")

await cex.download_data(coins=["BTC-USD", "ETH-USD"], intervals=["1d", "1h"], end_date="2022-01-01 00:00:00")
```

# Améliorations possibles
- Ajouter une date de début de récupération (actuellement fixée au 1er janvier 2017)
- Gérer d'autres datas récupérables via ccxt
