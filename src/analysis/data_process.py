import pandas as pd
import logging
import numpy as np
import matplotlib as plt
from typing import Optional, Tuple
from pathlib import Path
from arch import arch_model
from statsmodels.tsa.arima.model import ARIMA
from sklearn.preprocessing import MinMaxScaler


class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_to_dataframe(self, klines: list) -> Optional[pd.DataFrame]:
        try:
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume',
                     'close_time', 'quote_volume', 'trades',
                     'taker_buy_base', 'taker_buy_quote', 'ignore']
            
            df = pd.DataFrame(klines, columns=columns)

            #Conversión de tipos
            numeric_cols = ['open', 'high','low','close','volume', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, error='coerce')

            df['timestamps'] = pd.to_datetime(df['timestamp'], unit='ms')

            return df.set_index('timestamp')
        except Exception as e:
            self.logger.error(f"Error procesando datos: {str(e)}")
            return None
        
    def add_technical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        #Agregar features técnicas para el modelo predictivo
        try:
            #Returns y Volatilidad
            df['log_returns'] = np.log(df['close'] / df['close'].shift(1))   
            
            #Indicadores de mercado
            df['taker_buy_ratio'] = df['taker_buy_quote'] / df['quote_volume']
            df['liquidity_gap'] = (df['high'] - df['low']) / df['close']
            df['price_spread'] = df['close'] - df['open']

            #Medias móviles
            df['sma_20'] = df['close'].rolling(20).mean()
            df['ema_12'] = df['close'].ewm(span=12).mean()
            df['ema_26'] = df['close'].ewm(span=26).mean()
            df['macd'] = df['ema_12'] - df['ema_26']

            return df.dropna()
        except Exception as e:
            self.logger.error(f"Error añadiendo features: {str(e)}")
            return df

    def calculate_volatility(self, df: pd.DataFrame) -> pd.DataFrame:
        #Calcular métricas de volatilidad con GARCH
        try:
            returns = df['log_returns'].dropna()
            if len(returns) > 10:
                #Reescalado de retornos (problemas con resultados logaritmicos)
                scalated_returns = returns * 1000

                #Ajuste de modelo garch
                garch = arch_model(scalated_returns, vol='GARCH', p=1, q=1)
                garch_fitted = garch.fit(disp = 'off')

                #Volver a la escala original post calculo
                df['garch_volatility'] = garch_fitted.conditional_volatility / 1000
                return df
        except Exception as e:
            self.logger.warning(f"Error calculando GARCH: {str(e)}")
            df["garch_volatility"] = np.nan
            return df

    def prepare_training_data(self, df: pd.DataFrame, forecast_horizon: int=3 ) -> Tuple[pd.DataFrame, pd.Series]:
        #Prerarar datos para entrenamiento con target binario
        try:
            #Definición de target (1 si no cae > 5% en N días, 0 si cae)
            df['target'] = np.where(
                df['close'].shift(-forecast_horizon) < df['close'] * 0.95, 0, 1
            )
            features = [
                'log_returns','taker_buy_ratio', 'liquidity_gap','garch_volatility', 'macd', 'sma_20'
            ]

            #Escalado
            X = self.scaler.fit_transform(df[features].dropna())
            y = df['target'].dropna()

            return X, y
        except Exception as e:
            self.logger.error(f"Error preparando datos: {str(e)}")
            return None, None

    def save_data(self, df: pd.DataFrame, filename: str) -> bool:
        try:
            safe_path = Path('data/processed') / Path(filename).name
            df.to_parquet(safe_path)
            self.logger.info(f"Datos guardados en {safe_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error guardando datos: {str(e)}")
            return False
    
    def full_pipeline(self, klines: list, save_as: str = None) -> Optional[pd.DataFrame]:
        #Ejecuta todo el pipeline de procesamiento
        try:
            df = self.process_to_dataframe(klines)
            if df is None:
                return None
            
            df = self.add_technical_features(df)
            df = self.calculate_volatility(df)

            if save_as:
                self.save_data(df, save_as)

            return df
        except Exception as e:
            self.logger.error(f"Error en pipeline: {str(e)}")
            return None
        