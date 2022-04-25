# python-cexdata
Permet de récupérer sous forme de CSV l'historique des cours sur FTX et Binance.

Si des données partielles existent déjà, il ne récupère que ce qui manque (utile sur les timeframes les plus petits).

# Utilisation
Voir le fichier cexdata.ipynb
```python
from cexdata import CexData

cex = CexData("ftx")

await cex.download_data(coins=["BTC/USDT", "ETH/USDT"], intervals=["1d", "1h"], end_date="2022-01-01T00:00:00")
```

# Améliorations possibles
- Plus d'exchanges
- Ajouter une date de début de récupération (actuellement fixée au 1er janvier 2017)
- Gérer d'autres datas récupérables via ccxt
