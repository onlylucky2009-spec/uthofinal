import redis
import os
import time

# --- 1. REDIS CONNECTION SETUP (HEROKU SSL OPTIMIZED) ---
# Heroku uses 'rediss://' (secure). Standard Python redis needs certificate verification disabled 
# for Heroku's self-signed certificates.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

try:
    if REDIS_URL.startswith("rediss://"):
        # Heroku Production Environment
        r = redis.from_url(
            REDIS_URL, 
            decode_responses=True, 
            ssl_cert_reqs=None # Critical: Bypass self-signed cert error
        )
    else:
        # Local Development Environment
        r = redis.from_url(REDIS_URL, decode_responses=True)
except Exception as e:
    print(f"Failed to connect to Redis: {e}")
    r = None

# --- 2. LUA SCRIPT: ATOMIC TRADE LIMITING ---
# Yeh script Redis ke andar execute hoti hai. 
# Iska faayda yeh hai ki "Check" aur "Increment" ke beech koi dusra signal interrupt nahi kar sakta.
# Race conditions aur double trading se bachne ka yahi best tareeka hai.
LUA_TRADE_LIMIT_CHECK = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local current = redis.call('get', key)

if current and tonumber(current) >= limit then
    return 0 -- Trade Reject: Limit reached
else
    local new_val = redis.call('incr', key)
    -- Naya key hai toh 24 ghante (86400s) ka expiry set karein
    if tonumber(new_val) == 1 then
        redis.call('expire', key, 86400)
    end
    return 1 -- Trade Allow: Within limit
end
"""

class TradeControl:
    
    # --- TRADE LIMITS & LUA EXECUTION ---
    
    @staticmethod
    def can_trade(strategy_side: str, limit: int):
        """
        Check if a strategy (bull/bear/etc) has reached its daily trade limit.
        Uses Atomic Lua script.
        """
        if not r: return False
        
        # Daily unique key format: limit:bull:2025-12-22
        date_str = time.strftime("%Y-%m-%d")
        key = f"limit:{strategy_side}:{date_str}"
        
        try:
            lua_script = r.register_script(LUA_TRADE_LIMIT_CHECK)
            result = lua_script(keys=[key], args=[limit])
            return bool(result)
        except Exception as e:
            print(f"Trade Limit Logic Error: {e}")
            return False

    @staticmethod
    def get_current_count(strategy_side: str):
        """Aaj ke liye kitne trades ho chuke hain, yeh fetch karein."""
        if not r: return 0
        date_str = time.strftime("%Y-%m-%d")
        key = f"limit:{strategy_side}:{date_str}"
        try:
            val = r.get(key)
            return int(val) if val else 0
        except:
            return 0

    # --- CONFIG PERSISTENCE (API KEYS & SECRETS) ---
    # Inhe Redis mein save karne se Heroku restart par bhi API keys nahi udegi.

    @staticmethod
    def save_config(api_key, api_secret):
        """Save credentials permanently."""
        if not r: return False
        try:
            r.set("nexus:api_key", api_key)
            r.set("nexus:api_secret", api_secret)
            return True
        except Exception as e:
            print(f"Error saving API config: {e}")
            return False

    @staticmethod
    def get_config():
        """Restart ke baad API keys wapas load karne ke liye."""
        if not r: return None, None
        try:
            api_key = r.get("nexus:api_key")
            api_secret = r.get("nexus:api_secret")
            return api_key, api_secret
        except Exception as e:
            print(f"Error fetching API config: {e}")
            return None, None

    # --- SESSION PERSISTENCE (ACCESS TOKENS) ---
    # Access token 24 ghante tak valid rahega, bar-bar login ki zaroorat nahi.

    @staticmethod
    def save_access_token(token):
        """Save access token with 24h expiry."""
        if not r: return False
        try:
            r.set("nexus:access_token", token, ex=86400)
            return True
        except Exception as e:
            print(f"Error saving access token: {e}")
            return False

    @staticmethod
    def get_access_token():
        """Check if we have an active session in Redis."""
        if not r: return None
        try:
            return r.get("nexus:access_token")
        except Exception as e:
            print(f"Error fetching access token: {e}")
            return None