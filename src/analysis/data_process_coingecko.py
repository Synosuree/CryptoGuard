import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class CoinDataProcessor:
    @staticmethod
    def clean_coin_metrics(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        #Procesado de datos+
        logger.info('Procesando datos de la moneda...')
        
        processed = {
            'id': raw_data.get('id'),
            'symbol': raw_data.get('symbol'),
            'name': raw_data.get('name'),
            'dev_score': CoinDataProcessor.safe_float(raw_data.get('developer_score')),
            'community': CoinDataProcessor.safe_int(raw_data.get('community_data', {}).get('twitter_followers')),
            'liquidity': CoinDataProcessor.safe_float(raw_data.get('liquidity_score')),
            'ath_change': CoinDataProcessor.safe_float(
                raw_data.get('market_data', {}).get('ath_change_percentage', {}).get('usd')),
            'current_price': CoinDataProcessor.safe_float(
                raw_data.get('market_data', {}).get('current_price', {}).get('usd')),
            'market_cap': CoinDataProcessor.safe_float(
                raw_data.get('market_data', {}).get('market_cap', {}).get('usd')),
            'circulating_supply': CoinDataProcessor.safe_float(
                raw_data.get('market_data', {}).get('circulating_supply')),
            'total_supply': CoinDataProcessor.safe_float(
                raw_data.get('market_data', {}).get('total_supply')),
            'max_supply': CoinDataProcessor.safe_float(
                raw_data.get('market_data', {}).get('max_supply')),
            'last_updated': raw_data.get('last_updated')
        }

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
        #Proporción de suministro en circulación
        if data['total_supply'] and data['circulating_supply']:
            data['supply_ratio'] = data['circulating_supply'] / data['total_supply']
        else:
            data['supply_ratio'] = None

        #Valorización totalmente diluida
        if data['current_price'] and data['total_supply']:
            data['fdv'] = data['current_price'] / data['total_supply']
        else:
            data['fdv'] = None

        #Clasificación de liquidez
        if data['liquidity'] is not None:
            if data['liquidity'] > 0.8:
                data['liquidity_rating'] = 'High'
            elif data['liquidity'] > 0.5:
                data['liquidity_rating'] = 'Mid'
            else:
                data['liquidity_rating'] = 'Low'
        else:
            data['liquidity_rating'] = 'Unknown'
        
        return data
    
    @staticmethod
    def to_dataframe(clean_data: Dict[str, Any]) -> pd.DataFrame:
        logger.info('Convirtiendo datos en dataframe')

        df = pd.DataFrame([clean_data])
        
        df = df.replace({np.nan: None})

        numeric_cols = ['dev_score', 'liquidity', 'ath_change', 'current_price', 'market_cap', 'circulating_supply', 'total_supply', 'max_supply']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
        
        logger.debug(f'Dataframe generado con {len(df.columns)} columnas')
        return df
    
    @staticmethod
    def validate_data(clean_data: Dict[str, Any]) -> bool:
        #Validación de cumplimiento de requisitos minimos

        required_fields = ['id', 'symbol', 'current_price', 'market_cap']

        for field in required_fields:
            if not clean_data(field):
                logger.error(f'Campo requerido faltante: {field}')
                return False
            
        if clean_data['current_price'] <= 0:
            logger.error ("Precio Actual Invalido (<= 0)")
            return False
        
        return True
    
    @staticmethod    
    def null_precentage(df: pd.DataFrame) -> Optional[Any]:
        nulls = (df.isna().mean() * 100).round(2)
        return nulls       