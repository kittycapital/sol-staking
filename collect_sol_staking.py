#!/usr/bin/env python3
"""
Solana StakeHistory Sysvar Data Collector
=========================================
Fetches activating/deactivating stake data from Solana's StakeHistory sysvar
and combines with SOL price data from CoinGecko.

Usage:
  python3 collect_sol_staking.py

Output:
  data/sol_staking_data.json

Designed for GitHub Actions scheduled runs (every 6 hours).
"""

import json
import struct
import base64
import urllib.request
import urllib.error
import os
import sys
from datetime import datetime, timezone

# === CONFIG ===
SOLANA_RPC = os.environ.get('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com')
STAKE_HISTORY_PUBKEY = 'SysvarStakeHistory1111111111111111111111111'
COINGECKO_API = 'https://api.coingecko.com/api/v3'
OUTPUT_FILE = 'data/sol_staking_data.json'
LAMPORTS_PER_SOL = 1_000_000_000


def rpc_call(method, params=None):
    """Make a Solana JSON-RPC call."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or []
    }
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        SOLANA_RPC,
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            if 'error' in result:
                print(f"RPC Error: {result['error']}")
                return None
            return result.get('result')
    except Exception as e:
        print(f"RPC call failed: {e}")
        return None


def get_epoch_info():
    """Get current epoch information."""
    result = rpc_call('getEpochInfo')
    if result:
        return {
            'epoch': result['epoch'],
            'slotIndex': result['slotIndex'],
            'slotsInEpoch': result['slotsInEpoch'],
            'absoluteSlot': result['absoluteSlot']
        }
    return None


def parse_stake_history(data_bytes):
    """
    Parse StakeHistory sysvar binary data.
    
    Format: 
      - 8 bytes: length (u64, little-endian) = number of entries
      - For each entry:
        - 8 bytes: epoch (u64)
        - 8 bytes: effective (u64) in lamports
        - 8 bytes: activating (u64) in lamports
        - 8 bytes: deactivating (u64) in lamports
    """
    entries = []
    offset = 0
    
    # Read number of entries
    if len(data_bytes) < 8:
        print("Data too short for header")
        return entries
    
    num_entries = struct.unpack_from('<Q', data_bytes, offset)[0]
    offset += 8
    print(f"StakeHistory contains {num_entries} entries")
    
    entry_size = 32  # 4 x u64
    
    for i in range(num_entries):
        if offset + entry_size > len(data_bytes):
            break
        
        epoch, effective, activating, deactivating = struct.unpack_from('<QQQQ', data_bytes, offset)
        offset += entry_size
        
        entries.append({
            'epoch': epoch,
            'effective': effective / LAMPORTS_PER_SOL,
            'activating': activating / LAMPORTS_PER_SOL,
            'deactivating': deactivating / LAMPORTS_PER_SOL
        })
    
    # Sort by epoch ascending
    entries.sort(key=lambda x: x['epoch'])
    return entries


def fetch_stake_history():
    """Fetch and parse StakeHistory sysvar."""
    print("Fetching StakeHistory sysvar...")
    result = rpc_call('getAccountInfo', [
        STAKE_HISTORY_PUBKEY,
        {"encoding": "base64"}
    ])
    
    if not result or not result.get('value'):
        print("Failed to fetch StakeHistory")
        return []
    
    data_b64 = result['value']['data'][0]
    data_bytes = base64.b64decode(data_b64)
    print(f"StakeHistory data size: {len(data_bytes)} bytes")
    
    return parse_stake_history(data_bytes)


def epoch_to_approximate_date(epoch, current_epoch, current_date):
    """
    Convert epoch number to approximate date.
    Each epoch is ~2.4 days (432,000 slots × 400ms per slot).
    """
    epoch_duration_days = 2.4
    epochs_diff = current_epoch - epoch
    days_ago = epochs_diff * epoch_duration_days
    from datetime import timedelta
    approx_date = current_date - timedelta(days=days_ago)
    return approx_date.strftime('%Y-%m-%d')


def fetch_sol_price_history(days=1100):
    """Fetch SOL price history from CoinGecko."""
    print(f"Fetching SOL price history ({days} days)...")
    url = f"{COINGECKO_API}/coins/solana/market_chart?vs_currency=usd&days={days}&interval=daily"
    
    try:
        req = urllib.request.Request(url, headers={
            'Accept': 'application/json',
            'User-Agent': 'HerdVibe-Collector/1.0'
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            prices = {}
            for ts, price in data.get('prices', []):
                date_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                prices[date_str] = round(price, 2)
            print(f"Got {len(prices)} price data points")
            return prices
    except Exception as e:
        print(f"CoinGecko fetch failed: {e}")
        return {}


def load_existing_data():
    """Load existing data file if present."""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return None


def build_output(stake_entries, price_map, epoch_info):
    """Build the final JSON output."""
    current_date = datetime.now(timezone.utc)
    current_epoch = epoch_info['epoch']
    
    data = []
    for entry in stake_entries:
        date_str = epoch_to_approximate_date(entry['epoch'], current_epoch, current_date)
        sol_price = price_map.get(date_str, 0)
        
        data.append({
            'date': date_str,
            'epoch': entry['epoch'],
            'activating': round(entry['activating']),
            'deactivating': round(entry['deactivating']),
            'effective': round(entry['effective']),
            'netFlow': round(entry['activating'] - entry['deactivating']),
            'solPrice': sol_price
        })
    
    # Sort by date and deduplicate
    data.sort(key=lambda x: x['date'])
    seen_dates = set()
    unique_data = []
    for d in data:
        if d['date'] not in seen_dates:
            seen_dates.add(d['date'])
            unique_data.append(d)
    data = unique_data
    
    # Calculate stats
    if data:
        latest = data[-1]
        last_90 = data[-90:] if len(data) >= 90 else data
        act_vals = [d['activating'] for d in data]
        deact_vals = [d['deactivating'] for d in data]
        net_vals = [d['netFlow'] for d in data]
        
        # 90-day correlation
        import numpy as np
        if len(data) >= 90:
            recent_prices = np.array([d['solPrice'] for d in last_90 if d['solPrice'] > 0])
            recent_deact = np.array([d['deactivating'] for d in last_90 if d['solPrice'] > 0])
            if len(recent_prices) > 10:
                corr = float(np.corrcoef(recent_deact, recent_prices)[0, 1])
            else:
                corr = 0
        else:
            corr = 0
        
        stats = {
            'currentActivating': latest['activating'],
            'currentDeactivating': latest['deactivating'],
            'currentNetFlow': latest['netFlow'],
            'currentEpoch': latest['epoch'],
            'avgActivating': round(np.mean([d['activating'] for d in last_90])),
            'avgDeactivating': round(np.mean([d['deactivating'] for d in last_90])),
            'maxDeactivating': round(max(deact_vals)),
            'maxActivating': round(max(act_vals)),
            'correlation': round(corr, 4),
            'totalDataPoints': len(data)
        }
    else:
        stats = {}
    
    # Find inflection points
    inflection_points = find_inflection_points(data)
    
    output = {
        'meta': {
            'lastUpdated': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
            'source': 'Solana StakeHistory Sysvar (on-chain)',
            'rpcEndpoint': SOLANA_RPC.split('//')[1].split('/')[0] if '//' in SOLANA_RPC else SOLANA_RPC,
            'currentEpoch': current_epoch,
            'note': 'Activating=스테이킹 진입(매수 신호), Deactivating=언스테이킹 퇴출(매도 신호), units=SOL per epoch'
        },
        'stats': stats,
        'data': data,
        'inflectionPoints': inflection_points
    }
    
    return output


def find_inflection_points(data, window=30, min_gap_days=21):
    """Find peaks and troughs in net flow."""
    if len(data) < window * 2:
        return []
    
    import numpy as np
    import pandas as pd
    
    nf = pd.Series([d['netFlow'] for d in data])
    nf_smooth = nf.rolling(14, min_periods=1).mean()
    
    points = []
    p75 = np.percentile(nf_smooth, 75)
    p25 = np.percentile(nf_smooth, 25)
    
    for i in range(window, len(nf_smooth) - 1, 7):
        w = nf_smooth.iloc[max(0, i - window):i + 1]
        val = nf_smooth.iloc[i]
        
        if val == w.max() and val > p75:
            points.append({
                'date': data[i]['date'],
                'netFlow': round(float(val)),
                'deactivating': data[i]['deactivating'],
                'solPrice': data[i]['solPrice'],
                'type': 'peak_inflow'
            })
        elif val == w.min() and val < p25:
            points.append({
                'date': data[i]['date'],
                'netFlow': round(float(val)),
                'deactivating': data[i]['deactivating'],
                'solPrice': data[i]['solPrice'],
                'type': 'peak_outflow'
            })
    
    # Deduplicate close points
    filtered = []
    for pt in points:
        if not filtered or (
            pd.to_datetime(pt['date']) - pd.to_datetime(filtered[-1]['date'])
        ).days > min_gap_days:
            filtered.append(pt)
    
    return filtered


def main():
    print("=" * 50)
    print("Solana Staking Queue Data Collector")
    print("=" * 50)
    
    # 1. Get current epoch info
    epoch_info = get_epoch_info()
    if not epoch_info:
        print("FATAL: Cannot get epoch info")
        sys.exit(1)
    print(f"Current epoch: {epoch_info['epoch']}")
    print(f"Slot progress: {epoch_info['slotIndex']}/{epoch_info['slotsInEpoch']}")
    
    # 2. Fetch StakeHistory
    stake_entries = fetch_stake_history()
    if not stake_entries:
        print("FATAL: Cannot fetch StakeHistory")
        sys.exit(1)
    print(f"Parsed {len(stake_entries)} epoch entries")
    print(f"Epoch range: {stake_entries[0]['epoch']} ~ {stake_entries[-1]['epoch']}")
    
    # 3. Fetch SOL prices
    price_map = fetch_sol_price_history(days=1200)
    
    # 4. Build output
    output = build_output(stake_entries, price_map, epoch_info)
    
    # 5. Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f)
    
    print(f"\nSaved to {OUTPUT_FILE}")
    print(f"Data points: {len(output['data'])}")
    print(f"Inflection points: {len(output['inflectionPoints'])}")
    print("Done!")


if __name__ == '__main__':
    main()
