# Solana Token Pump/Dump Detection System - Backend API

## Overview
A Flask-based REST API that analyzes Solana token liquidity patterns to detect pre-pump and pre-dump manipulation signals. The system probes Jupiter's quote API with multiple trade sizes to map slippage curves and identify asymmetric liquidity structures that indicate upcoming price movements.

## Project Architecture

### Core Technology Stack
- **Python 3.11**: Backend runtime
- **Flask 2.3.3**: Web framework for REST API endpoints
- **flask-cors 4.0.0**: CORS handling for frontend integration
- **requests 2.31.0**: HTTP client for external API calls

### External APIs
- **Jupiter Quote API v6**: Solana DEX aggregator for slippage probing
- **CoinGecko API**: Real-time SOL/USD price data

## Key Features

### 1. Multi-Size Slippage Probing
- Tests trade execution at 6 different sizes: $100, $250, $500, $1,000, $2,500, $5,000
- Establishes baseline price with micro-trade (0.001 SOL)
- Probes both buy (SOL → Token) and sell (Token → SOL) directions
- Maps complete slippage curve to reveal liquidity depth structure

### 2. Pattern Detection Algorithms
The system detects four critical manipulation patterns:

**Liquidity Fortress (Pre-Pump Signal)**
- Sell slippage >> Buy slippage
- Asymmetry ratio > 1.7 (up to 2.5+ for critical)
- Indicates intentional sell resistance to prevent exit before pump

**Liquidity Cliff (Pre-Dump Signal)**
- Buy slippage >> Sell slippage  
- Asymmetry ratio < 0.6 (down to 0.4 for critical)
- Indicates removed support to enable fast dumping

**Compression Zone (Smart Money Positioning)**
- Buy slippage increases slower than trade size
- Deep liquidity placed at higher levels to absorb buying pressure
- Secondary pre-pump indicator

**Accelerating Cliff (Active Dump Preparation)**
- Sell slippage increases faster than expected
- Support being actively removed in real-time
- Indicates imminent dump

### 3. Time-Series Analysis
- Maintains last 10 measurements per token in memory
- Compares current asymmetry to historical data
- Detects active manipulation happening NOW (fortress building or cliff carving)
- Increases urgency score when rapid changes detected

### 4. Market State Classification
Final output classifies tokens into states:
- **PRE_PUMP** (Critical/High/Medium severity)
- **PRE_DUMP** (Critical/High/Medium severity)
- **HOLDING** (Stable, balanced liquidity)
- **UNCERTAIN** (Mixed signals)

Each classification includes:
- Confidence score (0-95%)
- Expected timeframe (2-60 minutes)
- Recommended action (Buy/Sell/Hold/Wait)
- List of detected pattern signals

### 5. Caching System
- 120-second cache duration per token
- In-memory storage (suitable for MVP)
- Returns cached results to optimize API usage
- Cache age included in response

## API Endpoints

### GET `/`
Health check with service information

### GET `/health`
Detailed system status including cache size and tokens tracked

### POST `/analyze`
Main analysis endpoint

**Request Body:**
```json
{
  "token_address": "SolanaTokenAddress..."
}
```

**Response:**
```json
{
  "state": "PRE_PUMP",
  "severity": "CRITICAL",
  "timeframe": "5-20 minutes",
  "confidence": 85,
  "action": "🚀 STRONG BUY SIGNAL - Entry opportunity detected",
  "signals": [
    "LIQUIDITY_FORTRESS: Extreme sell resistance - Asymmetry ratio 2.73x",
    "COMPRESSION_ZONE: Buy slippage compression - Deep liquidity placed"
  ],
  "scores": {
    "pre_pump_score": 80,
    "pre_dump_score": 0
  },
  "slippage_data": { ... },
  "token_address": "...",
  "timestamp": 1234567890,
  "cached": false
}
```

### POST `/clear-cache`
Clears all cached analyses and historical data

## Configuration Constants

```python
PROBE_SIZES_USD = [100, 250, 500, 1000, 2500, 5000]
CACHE_DURATION_SECONDS = 120
MAX_HISTORICAL_MEASUREMENTS = 10
SOL_MINT = 'So11111111111111111111111111111111111111112'
```

## Recent Changes

**2024-11-21**: Initial project setup
- Created Flask application with complete slippage analysis engine
- Implemented all pattern detection algorithms
- Set up CORS for React frontend integration
- Configured server to run on port 5000
- Added comprehensive logging system

## Project Structure

```
/
├── main.py              # Flask application with all analysis logic
├── requirements.txt     # Python dependencies
├── .gitignore          # Python project ignores
└── replit.md           # Project documentation (this file)
```

## Future Enhancements
- PostgreSQL database for persistent historical storage
- WebSocket support for real-time streaming updates
- Rate limiting and request throttling
- Batch analysis endpoint for multiple tokens
- Token metadata enrichment from Solana blockchain
- Alert system with configurable thresholds and webhooks
