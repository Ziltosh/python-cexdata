import asyncio
from posixpath import dirname
from pathlib import Path
import sys
import ccxt.async_support as ccxt
import pytz
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm.auto import tqdm


class CexData:
    intervals_mseconds = {"1m": 60000,
                          "2m": 120000,
                          "5m": 300000,
                          "15m": 900000,
                          "30m": 1800000,
                          "1h": 3600000,
                          "2h": 7200000,
                          "4h": 14400000,
                          "12h": 43200000,
                          "1d": 86400000}

    exchange_limits = {
        "binance": 1000,
        "ftx": 5000,
        # "HitBTC": 1000,
        # "Bitfinex": 10000,
        # "KuCoin": 1500
    }

    def __init__(self, cex, path_download="./") -> None:
        """
        La fonction prend une chaîne, et si la chaîne est 'binance' ou 'ftx', elle crée un objet ccxt
        pour cet échange.

        La fonction crée également un chemin vers un dossier appelé 'database' dans le répertoire parent
        du répertoire courant, et crée un sous-dossier dans ce dossier avec le nom de l'échange.

        La fonction crée également un objet barre de progression.

        :param cex: L'échange que vous souhaitez utiliser
        """
        self.cex_class = cex
        self.path_download = path_download
        if self.cex_class.lower() == 'binance':
            self.cex = ccxt.binance(config={'enableRateLimit': True})
        elif self.cex_class.lower() == 'ftx':
            self.cex = ccxt.ftx(config={'enableRateLimit': True})

        self.path_data = str(
            Path(os.path.join(dirname(__file__), self.path_download, self.cex_class)).resolve())
        os.makedirs(self.path_data, exist_ok=True)
        self.pbar = None

    def load_data(self, coin, interval, start_date, end_date) -> pd.DataFrame:
        """
        Cette fonction prend une paire, un intervalle, une date de début et une date de fin et renvoie
        une trame de données des données OHLCV pour cette paire

        :param coin: la paire pour laquelle vous souhaitez obtenir des données
        :param interval: l'intervalle de temps entre chaque point de données
        :param start_date: La date de début des données que vous souhaitez charger
        :param end_date: La date à laquelle vous souhaitez mettre fin à vos données
        """
        file_path = f"{self.path_data}/{interval}/"
        file_name = f"{file_path}{coin.replace('/', '-')}.csv"
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"Le fichier {file_name} n'existe pas")

        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, unit='ms')
        df = df.groupby(df.index).first()
        df = df.loc[start_date:end_date]
        df = df.iloc[:-1]
        del df["timestamp.1"]

        return df

    async def download_data(self,
                            coins,
                            intervals,
                            end_date=None):
        """
        Télécharge les données des API de CEX et les stocke dans des fichiers csv

        :param coins: une liste de paires pour lesquelles télécharger des données
        :param intervals: liste de chaînes, par ex. ['1h', '1d', '5m']
        :param end_date: la date d'arrêt du téléchargement des données. Si aucun, téléchargera les
        données jusqu'à la date actuelle
        """

        # if not start_date:
        start_date = datetime.fromisoformat(
            '2017-01-01 00:00:00')

        # else:
        #     start_date = datetime.fromisoformat(
        #         start_date)

        if not end_date:
            end_date = datetime.now(tz=pytz.utc)
        else:
            # Si la date passée est dans le futur on la mets a la date courante
            # Ca évite de toujours retélécharger des données qui n'existent pas encore
            end_date = min(datetime.fromisoformat(end_date), datetime.now())

        start_timestamp = int(start_date.timestamp() * 1000)

        coins = [str(coin).replace('-', '/')
                 for coin in coins]

        # Si c'est binance, on change la paire en XXX/USDT
        if self.cex_class == "binance":
            coins = [str(coin).replace('-', '/').replace('USD', 'USDT')
                     for coin in coins]

        for interval in intervals:

            all_dt_intervals = list(self.create_intervals(
                start_date, end_date, self.create_timedelta(interval)))
            last_dt = all_dt_intervals[-1].astimezone(pytz.utc)

            end_timestamp = int(last_dt.timestamp() * 1000)

            for coin in coins:
                print(
                    f"\tRécupération pour la paire {coin} en timeframe {interval} sur le CEX {self.cex_class}")

                file_path = f"{self.path_data}/{interval}/"
                os.makedirs(file_path, exist_ok=True)
                file_name = f"{file_path}{coin.replace('/', '-')}.csv"
                if self.cex_class.lower() == "binance":
                    file_name = file_name.replace('USDT', 'USD')

                dt_or_false = await self.is_data_missing(file_name, last_dt)
                # print(dt_or_false)

                if dt_or_false:

                    print("\tTéléchargement des données")

                    tasks = []
                    current_timestamp = int(dt_or_false.timestamp() * 1000)

                    while True:
                        tasks.append(asyncio.create_task(self.download_tf(
                            coin, interval, current_timestamp)))

                        current_timestamp = min([current_timestamp + int(CexData.exchange_limits[self.cex_class]) *
                                                 int(CexData.intervals_mseconds[interval]), end_timestamp])
                        if current_timestamp >= end_timestamp:
                            break

                    self.pbar = tqdm(tasks)
                    results = await asyncio.gather(*tasks)
                    await self.cex.close()
                    self.pbar.close()

                    allDf = []
                    for i in results:
                        # Si on n'a aucune donnée on ne fait rien
                        if i:
                            allDf.append(pd.DataFrame(i))

                    # Si il y a des données
                    if allDf:
                        final = pd.concat(allDf, ignore_index=True, sort=False)
                        final.columns = ['timestamp', 'open',
                                         'high', 'low', 'close', 'volume']
                        final.rename(
                            columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
                        final = final.set_index(final['timestamp'], drop=True)
                        final.index = pd.to_datetime(final.index, unit='ms')
                        final = final.groupby(final.index).first()

                        if os.path.exists(file_name):
                            with open(file_name, mode='a') as f:
                                final.iloc[1:].to_csv(
                                    path_or_buf=f, header=False)
                        else:
                            with open(file_name, mode='w') as f:
                                final.to_csv(path_or_buf=f)
                    else:
                        print(
                            f"\tPas de données pour {coin} en {interval} sur cette période")
                else:
                    print("\tDonnées déjà récupérées")

    async def download_tf(self, coin, interval, start_timestamp):
        """
        Télécharge les données de l'API et les stocke dans une trame de données.

        :param coin: la pièce pour laquelle vous souhaitez télécharger des données
        :param interval: l'intervalle de temps des données que vous souhaitez télécharger
        :param start_timestamp: l'heure de début des données que vous souhaitez télécharger
        """
        essais = 1
        while essais < 3:
            try:
                r = await self.cex.fetch_ohlcv(
                    symbol=coin, timeframe=interval, since=start_timestamp, limit=CexData.exchange_limits[self.cex_class])

                self.pbar.update(1)
                return r
            except:
                essais += 1
                if essais == 3:
                    raise TooManyError

    async def is_data_missing(self, file_name, last_dt):
        """
        Cette fonction vérifie s'il y a des données manquantes dans la base de données pour une pièce,
        un intervalle et une plage de temps donnés

        :param file_name: Le nom du fichier pour vérifier les données manquantes
        :last_dt: La date de fin de la plage de données voulue
        """
        if os.path.isfile(file_name):
            df = pd.read_csv(file_name, index_col=0, parse_dates=True)
            df.index = pd.to_datetime(df.index, unit='ms')
            df = df.groupby(df.index).first()

            if pytz.utc.localize(df.index[-1]) >= last_dt:
                return False
        else:
            # Le fichier n'existe pas, on renvoie la date de début
            return datetime.fromisoformat('2017-01-01')

        return pytz.utc.localize(df.index[-1])

    def create_intervals(self, start_date, end_date, delta):
        """
        Étant donné une date de début, une date de fin et un delta de temps, créez une liste de tuples
        de la forme (start_date, end_date) où chaque tuple représente un intervalle de temps de longueur
        delta

        :param start_date: La date de début de l'intervalle
        :param end_date: La date de fin de l'intervalle
        :param delta: un objet timedelta qui représente la longueur de l'intervalle
        """
        current = start_date
        while current <= end_date:
            yield current
            current += delta

    def create_timedelta(self, interval):
        """
        Retourne un timedelta en fonction de l'intervalle donné

        :param interval: L'intervalle de temps à utiliser dans timedelta
        """
        if interval == '1y':
            return timedelta(days=365)
        elif interval == '1M':
            return timedelta(days=30)
        elif interval == '1w':
            return timedelta(days=7)
        elif interval == '1d':
            return timedelta(days=1)
        elif interval == '4h':
            return timedelta(hours=4)
        elif interval == '1h':
            return timedelta(hours=1)
        elif interval == '5m':
            return timedelta(minutes=5)
        elif interval == '1m':
            return timedelta(minutes=1)
        else:
            raise ValueError(f"Intervalle {interval} inconnu")


class TooManyError(Exception):
    pass
