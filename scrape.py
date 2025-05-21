# Updated fetch_bonuses method in scrape.py
def fetch_bonuses(self, url, auth_data, csv_file='bonuses.csv'):
    payload = {
        'module': '/users/syncData',
        'merchantId': auth_data['merchantID'],
        'domainId': '0',
        'accessId': auth_data['access_id'],
        'accessToken': auth_data['token'],
        'walletIsAdmin': ''
    }
    bonuses = []
    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = requests.post(auth_data['api_url'], data=payload, timeout=10).json()
            bonuses = res.get('data', {}).get('bonus', []) + res.get('data', {}).get('promotions', [])
            break
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            if attempt == max_retries - 1:
                emit_event("scrape_failed", {"url": url, "error": str(e)})
                return 0
            time.sleep(2 ** attempt)
    
    # New fields: expiryDate, wageringRequirement, isCashable
    fieldnames = [
        'URL', 'merchantName', 'id', 'name', 'transactionType', 'bonusFixed', 'amount',
        'minWithdraw', 'maxWithdraw', 'withdrawToBonusRatio', 'rollover', 'balance',
        'claimConfig', 'claimCondition', 'bonus', 'bonusRandom', 'reset',
        'minTopup', 'maxTopup', 'referLink', 'expiryDate', 'wageringRequirement', 'isCashable'
    ]
    
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if f.tell() == 0:
            writer.writeheader()
        for b in bonuses:
            try:
                min_w = float(b.get('minWithdraw', 0) or 0
                bonus_f = float(b.get('bonusFixed', 0) or 0
                ratio = min_w / bonus_f if bonus_f else None
                expiry = datetime.datetime.fromtimestamp(b.get('expiryDate')).isoformat() if b.get('expiryDate') else None
            except:
                ratio = None
                expiry = None
            
            writer.writerow({
                **{k: b.get(k, '') for k in ['id', 'name', 'transactionType', 'bonusFixed', 'amount',
                                            'minWithdraw', 'maxWithdraw', 'rollover', 'balance',
                                            'claimConfig', 'claimCondition', 'bonus', 'bonusRandom',
                                            'reset', 'minTopup', 'maxTopup', 'referLink']},
                'URL': url,
                'merchantName': auth_data['merchantName'],
                'withdrawToBonusRatio': ratio,
                'expiryDate': expiry,
                'wageringRequirement': b.get('wageringRequirement', 'Not Specified'),
                'isCashable': b.get('isCashable', False)
            })
    return len(bonuses)