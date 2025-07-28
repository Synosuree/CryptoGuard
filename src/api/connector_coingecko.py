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
       """Obtiene todos los datos crudos de la moneda con parámetros optimizados"""
       logger.info(f"Obteniendo datos completos para {coin_id}")
       params = {
            'localization': 'false',
            'tickers': 'true',  # Habilitado para obtener datos de exchanges
            'market_data': 'true',
            'community_data': 'true',
            'developer_data': 'true',
            'sparkline': 'false'
            }
       return self._make_request(f"/coins/{coin_id}", params=params)
    
    def get_coin_metrics(self, coin_id: str) -> Dict[str, Any]:
        """Versión corregida que mapea exactamente la estructura de la API"""
        raw_data = self.get_coin_data(coin_id)
        
        # Debug: Ver estructura completa
        logger.debug("Estructura completa recibida:")
        for key in raw_data.keys():
            logger.debug(f"- {key}")
        
        # Extracción robusta con valores por defecto
        market_data = raw_data.get('market_data', {})
        tickers_data = raw_data.get('tickers', [])
        developer_data = raw_data.get('developer_data', {})

        tickers_info = [
            {
                'last': t.get('last'),
                'volume': t.get('volume'),
                'trust_score': t.get('trust_score')
            }
            for t in tickers_data if isinstance(t, dict)
        ]
        
        return {
            # Identificación
            'id': raw_data.get('id'),
            'symbol': raw_data.get('symbol'),
            'name': raw_data.get('name'),
            
            # Datos de desarrolladores
            'dev_stars': developer_data.get('stars'),  #Confianza en developers
            'forks': developer_data.get('forks'), #Actividad en el desarrollo
            'pull_merge_request': developer_data.get('pull_requests_merged'), #Actividad en el desarrollo

            
            
            # Comunidad y Sentimiento
            ## Indicador de sentimiento alcista o bajista entre traders
            'sentiment_up': raw_data.get('sentiment_votes_up_percentage'),
            'sentiment_dwn': raw_data.get('sentiment_votes_down_percentage'),
            #N° usuarios que siguen CC en CoinGecko / Interes puede correlacionarse con interés del mercado
            'watch_list': raw_data.get('watchlist_portfolio_users'), 
            
            # Liquidez y Volumen
            'liquidity_score': raw_data.get('liquidity_score'),
            'total_volume': market_data.get('total_volume', {}).get('usd'), # +Volumen +Liquidez
            
            # Mercado
            'current_price': market_data.get('current_price', {}).get('usd'), #Variable Obj (target)
            'high_24h': market_data.get('high_24h',{}).get('usd'),
            'low_24h': market_data.get('low_24h',{}).get('usd'),
            'price_change%_24h': market_data.get('price_change_percentage_24h'),
            
            'market_cap': market_data.get('market_cap', {}).get('usd'), #Tamaño y estabilidad del mercado
            'circulating_supply': market_data.get('circulating_supply'), #Monedas en circulación
            'total_supply': market_data.get('total_supply'),
            'max_supply': market_data.get('max_supply'), #Escasez programada es un factor deflacionario clave
            
            # Datos Historicos
            'ath': market_data.get('ath', {}).get('usd'),
            'ath_change': market_data.get('ath_change_percentage', {}).get('usd'),
            'atl': market_data.get('atl',{}).get('usd'),
            'price_1d_ago': market_data.get('price_change_percentage_24h_in_currency',{}).get('usd'),
            'price_7d_ago': market_data.get('price_change_percentage_7d_in_currency', {}).get('usd'),


            # Actividad de red
            'block_time': raw_data.get('block_time_in_minutes'), #Afecta la emisión de nuevos CC y costos de minería.
            'hashing_alg': raw_data.get('hashing_algorithm'), #Seguridad de la red -> impacta confianza}

            # Datos de Exchanges
            'tickers':tickers_info,         
            
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