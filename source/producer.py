```python
"""
Real-time Market Data Producer
Simulates high-frequency trading data for stocks
"""
import json
import time
import random
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'JPM', 'BAC']
        
    def generate_tick(self, ticker):
        """Generate a single market tick"""
        base_price = random.uniform(100, 500)
        volatility = random.uniform(0.001, 0.01)
        
        return {
            'ticker': ticker,
            'timestamp': datetime.utcnow().isoformat(),
            'price': round(base_price * (1 + random.uniform(-volatility, volatility)), 2),
            'volume': random.randint(100, 10000),
            'bid': round(base_price * 0.999, 2),
            'ask': round(base_price * 1.001, 2),
            'exchange': random.choice(['NYSE', 'NASDAQ', 'BATS']),
            'trade_type': random.choice(['BUY', 'SELL', 'HOLD'])
        }
    
    def run(self):
        """Run producer"""
        logger.info("Starting Market Data Producer...")
        
        for i in range(1000):  # Generate 1000 ticks
            ticker = random.choice(self.tickers)
            data = self.generate_tick(ticker)
            print(f"Tick {i}: {data}")
            time.sleep(0.1)  # 10 ticks per second

if __name__ == "__main__":
    producer = MarketDataProducer()
    producer.run()
