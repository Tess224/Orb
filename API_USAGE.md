# Orb API - Usage Guide

## 🎯 Rate Limiting System

### Anonymous Users (No Access Code)
Everyone can use the API **without an access code**!

- **Limit:** 10 analyses per day
- **No registration required**
- **Free and open**

#### Example Request (Anonymous):
```bash
curl -X POST https://your-api.railway.app/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "token_address": "YourSolanaTokenAddressHere"
  }'
```

---

### Premium Users (With Access Code)
Get higher limits by using an access code provided by the admin.

- **Custom limits** (e.g., 50, 100, or unlimited)
- **Same API, just add access_code field**

#### Example Request (With Access Code):
```bash
curl -X POST https://your-api.railway.app/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "token_address": "YourSolanaTokenAddressHere",
    "access_code": "PREMIUM-USER-1"
  }'
```

---

## 📊 Available Endpoints

### 1. Token Analysis
**Endpoint:** `POST /analyze`

**Request:**
```json
{
  "token_address": "So11111111111111111111111111111111111111112",
  "access_code": "OPTIONAL-ACCESS-CODE"
}
```

**Response:**
```json
{
  "status": "success",
  "token_address": "So11111...",
  "liquidity_usd": 1234567,
  "volume_24h_usd": 987654,
  "signal": {
    "direction": "PRE_PUMP",
    "urgency": "HIGH",
    "confidence": 0.85
  },
  "cached": false,
  "timestamp": 1234567890
}
```

---

### 2. Wallet Analysis
**Endpoint:** `POST /api/wallet/analyze`

**Request:**
```json
{
  "wallet_address": "YourSolanaWalletAddressHere",
  "access_code": "OPTIONAL-ACCESS-CODE"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "iq": 75,
    "winRate": "65.5",
    "trades": 15,
    "pattern": "Calculated Trader"
  }
}
```

---

## 🔑 Getting an Access Code

Contact the admin to get a premium access code with higher limits.

**Benefits of access codes:**
- Higher daily limits (50, 100, or more)
- Priority processing
- Extended analysis history

---

## ⚠️ Rate Limit Response

When you exceed your daily limit:

```json
{
  "error": "Daily analysis limit exceeded",
  "limit": 10,
  "remaining": 0,
  "resets_at": 1234567890,
  "message": "You have used all 10 daily analyses."
}
```

**HTTP Status:** `429 Too Many Requests`

**What to do:**
1. Wait for the reset (24 hours from first request)
2. Get an access code for higher limits
3. Cache results to avoid duplicate requests

---

## 💡 Best Practices

### 1. Cache Results
Don't request the same token multiple times - cache the results!

### 2. Check Rate Limits
The API returns your remaining analyses in the response headers.

### 3. Handle Errors Gracefully
```javascript
try {
  const response = await fetch('/analyze', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      token_address: tokenAddress,
      access_code: userAccessCode  // Optional
    })
  });

  if (response.status === 429) {
    // Rate limit exceeded
    const data = await response.json();
    alert(`Limit reached! Resets at ${new Date(data.resets_at * 1000)}`);
  }
} catch (error) {
  console.error('API Error:', error);
}
```

### 4. Share Access Codes Securely
- Don't expose access codes in frontend code
- Store them in environment variables
- Make API calls from your backend

---

## 🚀 Example: React/Next.js Integration

```typescript
// lib/orbApi.ts
const ORB_API_URL = process.env.NEXT_PUBLIC_ORB_API_URL;
const ACCESS_CODE = process.env.ORB_ACCESS_CODE; // Backend only!

export async function analyzeToken(tokenAddress: string) {
  const response = await fetch(`${ORB_API_URL}/analyze`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      token_address: tokenAddress,
      // Only include access_code if calling from backend
      ...(ACCESS_CODE && { access_code: ACCESS_CODE })
    })
  });

  if (!response.ok) {
    if (response.status === 429) {
      const data = await response.json();
      throw new Error(`Rate limit exceeded. Resets at ${new Date(data.resets_at * 1000).toLocaleString()}`);
    }
    throw new Error('Analysis failed');
  }

  return response.json();
}
```

---

## 📞 Support

For access codes or technical support:
- Contact: [Your support email/Discord]
- Documentation: [Your docs URL]

---

## 🔒 Security Note

**Never expose access codes in:**
- Frontend JavaScript
- Git repositories
- Client-side code
- Public URLs

Always make authenticated requests from your backend!
