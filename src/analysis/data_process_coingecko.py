import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)

class CoinDataProcessor:
    @staticmethod
    #Esta función hace uso de calculate_derived_metrics
    def process_metrics(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        #Procesado de datos
        logger.info('Procesando datos de la moneda...')
        
        # Validación de tipos solo floats
        float_keys=['dev_stars', 'forks', 'pull_merge_request', 'sentiment_up', 'sentiment_dwn',
                    'watch_list', 'liquidity_score', 'total_volume', 'current_price', 'high_24h',
                    'low_24h', 'market_cap', 'circulating_supply', 'total_supply', 'max_supply',
                    'ath', 'atl', 'price_1d_ago', 'price_7d_ago']

        for key in float_keys:
                raw_data[key] = CoinDataProcessor.safe_float(raw_data.get(key))
        processed = raw_data
        #Calculo de métricas derivadas
        processed = CoinDataProcessor.calculate_derived_metrics(processed)

        logger.debug(f'Datos Procesados: {processed}')
        return processed
    
    @staticmethod
    def safe_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f'Valor no convertible a int: {value}')
            return None
        
    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except(ValueError, TypeError):
            logger.warning(f'Valor no convertible a float: {value}')
            return None
        
    @staticmethod
    def calculate_derived_metrics(data: Dict[str, Any]) -> Dict[str, Any]:
        # Proporción de suministro en circulación
        if data['total_supply'] and data['circulating_supply']:
            data['supply_ratio'] = data['circulating_supply'] / data['total_supply']
        else:
            data['supply_ratio'] = None

        # Fully Diluted Valuation (precio * total_supply)
        if data['current_price'] and data['total_supply']:
            data['fdv'] = data['current_price'] * data['total_supply']
        else:
            data['fdv'] = None

        # Volatilidad relativa en 24h
        if data['high_24h'] and data['low_24h'] and data['current_price']:
            data['range_24h_pct'] = (data['high_24h'] - data['low_24h']) / data['current_price']
        else:
            data['range_24h_pct'] = None

        # Volumen / Market Cap ratio
        if data['total_volume'] and data['market_cap']:
            data['vmkr'] = data['total_volume'] / data['market_cap']
        else:
            data['vmkr'] = None

        # Distancia al ATH/ATL
        if data['ath'] and data['current_price']:
            data['ath_distance_pct'] = (data['ath'] - data['current_price']) / data['ath']
        else:
            data['ath_distance_pct'] = None

        if data['atl'] and data['current_price']:
            data['atl_distance_pct'] = (data['current_price'] - data['atl']) / data['atl']
        else:
            data['atl_distance_pct'] = None

        # Momentum
        if data['price_1d_ago'] and data['current_price']:
            data['momentum_1d'] = data['current_price'] / data['price_1d_ago'] - 1
        else:
            data['momentum_1d'] = None

        if data['price_7d_ago'] and data['current_price']:
            data['momentum_7d'] = data['current_price'] / data['price_7d_ago'] - 1
        else:
            data['momentum_7d'] = None

        # Circulating ratio
        if data['max_supply'] not in (None, 0):
            data['circulating_ratio'] = data['circulating_supply'] / data['max_supply']
            data['has_max_supply'] = 1
        else:
            data['has_max_supply'] = 0

        # Inflación
        if data['total_supply'] and data['circulating_supply']:
            data['inflation_ratio'] = (data['total_supply'] - data['circulating_supply']) / data['total_supply']
        else:
            data['inflation_ratio'] = None

        # Sentimiento
        if data['sentiment_dwn'] is not None:
            data['sentiment_ratio'] = data['sentiment_up'] / (data['sentiment_dwn'] + 1e-6)
        else:
            data['sentiment_ratio'] = None

        if data['watch_list'] and data['market_cap']:
            data['wtchlist_per_mcap'] = data['watch_list'] / (data['market_cap'] + 1e-6)
        else:
            data['wtchlist_per_mcap'] = None

        # Liquidez
        if data['liquidity_score'] is not None:
            if data['liquidity_score'] > 0.8:
                data['liquidity_rating'] = 'High'
            elif data['liquidity_score'] > 0.5:
                data['liquidity_rating'] = 'Mid'
            else:
                data['liquidity_rating'] = 'Low'
        else:
            data['liquidity_rating'] = 'Unknown'

        return data
    @staticmethod
    #Al escalar se usa to_dataframe
    def scale_data(data: Dict[str, Any]) -> pd.DataFrame:
        logger.info('Escalando datos y convertiendo en dataframe')
        df = CoinDataProcessor.to_dataframe(data)

        #variables que no se deben escalar 
        non_scalable_cols = ['id', 'symbol', 'name', 'liquidity_rating', 'liquidity_score']
        
        numeric_cols = [
            col for col in  df.select_dtypes(include=[np.number]).columns
            if col not in non_scalable_cols
        ]
        if df.shape[0] <= 1:
            logger.warning('Solo hay una fila, se omite el escalado')
            return df

        if not numeric_cols:
            logger.warning('No hay columnas numéricas para escalar')
            return df
        
        scaler = MinMaxScaler()
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
        
        logger.info(f'{len(numeric_cols)} columnas escalas con MinMaxScaler')
        return df



    
    @staticmethod
    def to_dataframe(clean_data: Dict[str, Any]) -> pd.DataFrame:
        logger.info('Convirtiendo datos en DataFrame')

        df = pd.DataFrame([clean_data])

        exclude_cols = ['id', 'symbol', 'name', 'hashing_alg', 'tickers', 'last_updated', 'liquidity_rating']
        # Detectar y convertir automáticamente columnas numéricas
        for col in df.columns :
            if col not in exclude_cols:
                try:
                    # Intentar convertir si el valor es tipo numérico o str convertible
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception:
                    continue  # Deja tal cual si no se puede convertir

        # Manejar fechas si está presente
        if 'last_updated' in df.columns:
            try:
                df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
            except Exception as e:
                logger.warning(f"Error convirtiendo 'last_updated': {str(e)}")

        # Opcional: remplazar NaN por None (para compatibilidad con otras herramientas)
        df = df.replace({np.nan: None})

        logger.debug(f'DataFrame generado con {len(df.columns)} columnas')
        return df
    
    @staticmethod
    def validate_data(clean_data: Dict[str, Any]) -> bool:
        #Validación de cumplimiento de requisitos minimos

        required_fields = ['id', 'symbol', 'current_price', 'market_cap']

        for field in required_fields:
            if field not in clean_data:
                logger.error(f'Campo requerido faltante: {field}')
                return False
            if clean_data[field] in (None, '', 0):
                logger.error(f'Campo requerido inválido o vacío: {field}')

        if clean_data['current_price'] <= 0:
            logger.error('Precio actual inválido (<= 0)')
            return False
        
        return True
    
    @staticmethod    
    def null_precentage(df: pd.DataFrame) -> Optional[Any]:
        nulls = (df.isna().mean() * 100).round(2)
        return nulls       
    
    
    @staticmethod
    def full_pipeline(raw_metrics: Dict[str, Any]) -> pd.DataFrame:
        if not CoinDataProcessor.validate_data(raw_metrics):
            raise ValueError('Las métricas no son válidas o están incompletas')
        
        processed = CoinDataProcessor.process_metrics(raw_metrics)
        df = CoinDataProcessor.scale_data(processed)
        return df
