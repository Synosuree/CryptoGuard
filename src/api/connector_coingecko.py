import os
from dotenv import load_dotenv
import logging
import requests
from typing import Dict, Any, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CoinGeckoClient:
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self, timeout: int = 30):
        load_dotenv()
        self.api_key = os.getenv('COINGECKO_API_KEY')
        self.timeout = timeout
        
        if not self.api_key:
            logger.warning("COINGECKO_API_KEY no encontrada - Usando modo público (rate limit reducido)")
        
        self.session = self._configure_session()
        logger.info("CoinGeckoClient inicializado correctamente")

    def _configure_session(self) -> requests.Session:
        session = requests.Session()
        if self.api_key:
            session.headers.update({
                'x-cg-demo-api-key': self.api_key,
                'Accept': 'application/json'
            })
        return session
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        try:
            logger.debug(f"Solicitando: {url}")
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            # Debug: Ver respuesta cruda
            logger.debug(f"Respuesta cruda: {response.text[:200]}...")
            
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {response.status_code}: {e}\nRespuesta: {response.text}")
            raise
        except Exception as e:
            logger.error(f"Error en request: {str(e)}")
            raise
        
    #Para Debugg
    def get_coin_data(self, coin_id: str) -> Dict[str, Any]:
        """Obtiene TODOS los datos crudos de la moneda"""
        logger.info(f"Obteniendo datos completos para {coin_id}")
        return self._make_request(f"/coins/{coin_id}?localization=false&tickers=false&market_data=true&community_data=true&developer_data=true")
    
    def get_coin_metrics(self, coin_id: str) -> Dict[str, Any]:
        """Versión corregida que mapea exactamente la estructura de la API"""
        raw_data = self.get_coin_data(coin_id)
        
        # Debug: Ver estructura completa
        logger.debug("Estructura completa recibida:")
        for key in raw_data.keys():
            logger.debug(f"- {key}")
        
        # Extracción robusta con valores por defecto
        market_data = raw_data.get('market_data', {})
        community_data = raw_data.get('community_data', {})
        developer_data = raw_data.get('developer_data', {})
        
        return {
            # Identificación
            'id': raw_data.get('id'),
            'symbol': raw_data.get('symbol'),
            'name': raw_data.get('name'),
            
            # Datos de desarrolladores
            'dev_score': developer_data.get('score'),  # Cambiado de 'developer_score'
            
            # Comunidad
            'twitter_followers': community_data.get('twitter_followers'),  # Cambiado de 'community'
            
            # Liquidez
            'liquidity_score': raw_data.get('liquidity_score'),
            
            # Mercado
            'current_price': market_data.get('current_price', {}).get('usd'),
            'ath_change': market_data.get('ath_change_percentage', {}).get('usd'),  # Corregido de 'ath_score'
            'market_cap': market_data.get('market_cap', {}).get('usd'),
            'circulating_supply': market_data.get('circulating_supply'),
            'total_supply': market_data.get('total_supply'),
            'max_supply': market_data.get('max_supply'),
            
            # Metadata
            'last_updated': raw_data.get('last_updated')
        }
    
    def get_clean_metrics(self, coin_id: str) -> Dict[str, Any]:
        """Obtiene métricas validadas y procesadas"""
        metrics = self.get_coin_metrics(coin_id)
        
        # Validación básica
        if not metrics['id']:
            raise ValueError(f"Datos inválidos recibidos para {coin_id}")
        
        # Procesamiento adicional
        metrics['supply_ratio'] = (
            metrics['circulating_supply'] / metrics['total_supply']
            if metrics.get('circulating_supply') and metrics.get('total_supply')
            else None
        )
        
        return metrics