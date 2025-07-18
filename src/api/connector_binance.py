import os 
from dotenv import load_dotenv
from binance.client import Client
import logging
from typing import Optional, Dict
import pandas as pd

#Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceClient:
    def __init__(self):
        load_dotenv()

        self.api_key = os.getenv('BINANCE_API_KEY')
        self.api_secret = os.getenv('BINANCE_API_SECRET')

        if not self.api_key or not self.api_secret:
            logger.error("Credenciales no encontradas")
            raise ValueError("Credenciales Requeridas")
        
        self.client = self.CreateClient()
    
    def CreateClient(self) -> Client:
        try:
            return Client(
                api_key = self.api_key,
                api_secret = self.api_secret,
                requests_params = {"timeout":10},
                tld='com',
                testnet = False
            )
        except Exception as e:
            logger.error(f"Error en la creación del cliente: {str(e)}")
            raise

    def getHistoricalData(self, symbol:str, interval:str, days: int = 30) -> Optional[Dict]:
        try:
            if not isinstance(symbol, str) or not symbol.isupper():
                raise ValueError("Símbolo debe ser en mayúsculas (ej: BTCUSDT)")
            
            #obtención de data
            klines = self.client.get_historical_klines(
                symbol = symbol,
                interval = interval,
                start_str = f"{days} days ago UTC"
            )

            return self.processKlines(klines)
        except Exception as e:
            logger.error(f"Error obteniendo datos para {symbol}: {str(e)}")
            return None
        
    @staticmethod
    def processKlines(klines) -> Dict:
        cols =  ['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                'close_time', 'quote_volume', 'trades', 
                'taker_buy_base', 'taker_buy_quote', 'ignore']
        df = pd.DataFrame(klines, columns=cols)

         # Convertir tipos de datos
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 
                    'quote_volume', 'taker_buy_base', 'taker_buy_quote']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, axis=1)
        
        return df.set_index('timestamp')
