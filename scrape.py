import requests, csv, os

class Scraper:
    def __init__(self, setup):
        self.setup = setup

    def fetch_downlines(self, url, auth_data, csv_file='downlines.csv'):
        page = 0
        written = set()
        if os.path.exists(csv_file):
            with open(csv_file, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                written = {tuple(row.values()) for row in reader}
        total_new_rows = 0
        while True:
            payload = {
                'level': '1',
                'pageIndex': str(page),
                'module': '/referrer/getDownline',
                'merchantId': auth_data['merchantID'],
                'domainId': '0',
                'accessId': auth_data['access_id'],
                'accessToken': auth_data['token'],
                'walletIsAdmin': True
            }
            try:
                res = requests.post(auth_data['api_url'], data=payload).json()
            except:
                break
            if res.get('status') != "SUCCESS":
                break
            new_rows = []
            for d in res['data'].get('downlines', []):
                row = {
                    'url': url,
                    'id': d.get('id'),
                    'name': d.get('name'),
                    'count': d.get('count'),
                    'amount': d.get('amount'),
                    'registerDateTime': d.get('registerDateTime')
                }
                key = tuple(row.values())
                if key not in written:
                    new_rows.append(row)
                    written.add(key)
            if not new_rows:
                break
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=new_rows[0].keys())
                if f.tell() == 0:
                    writer.writeheader()
                writer.writerows(new_rows)
            total_new_rows += len(new_rows)
            page += 1
        return total_new_rows

    def fetch_bonuses(self, url, auth_data, csv_file='bonuses.csv'):
        payload = {
            'module': '/users/syncData',
            'merchantId': auth_data['merchantID'],
            'domainId': '0',
            'accessId': auth_data['access_id'],
            'accessToken': auth_data['token'],
            'walletIsAdmin': ''
        }
        try:
            res = requests.post(auth_data['api_url'], data=payload).json()
        except:
            return 0
        bonuses = res.get('data', {}).get('bonus', []) + res.get('data', {}).get('promotions', [])
        if not bonuses:
            return 0
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            fieldnames = [
                'URL', 'merchantName', 'id', 'name', 'transactionType', 'bonusFixed', 'amount',
                'minWithdraw', 'maxWithdraw', 'withdrawToBonusRatio', 'rollover', 'balance',
                'claimConfig', 'claimCondition', 'bonus', 'bonusRandom', 'reset',
                'minTopup', 'maxTopup', 'referLink'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if f.tell() == 0:
                writer.writeheader()
            for b in bonuses:
                try:
                    min_w = float(b.get('minWithdraw', 0) or 0)
                    bonus_f = float(b.get('bonusFixed', 0) or 0)
                    ratio = min_w / bonus_f if bonus_f else None
                except:
                    ratio = None
                writer.writerow({
                    'URL': url,
                    'merchantName': auth_data['merchantName'],
                    'id': b.get('id'), 'name': b.get('name'), 'transactionType': b.get('transactionType'),
                    'bonusFixed': b.get('bonusFixed'), 'amount': b.get('amount'),
                    'minWithdraw': b.get('minWithdraw'), 'maxWithdraw': b.get('maxWithdraw'),
                    'withdrawToBonusRatio': ratio, 'rollover': b.get('rollover'),
                    'balance': '', 'claimConfig': b.get('claimConfig'),
                    'claimCondition': b.get('claimCondition'), 'bonus': b.get('bonus'),
                    'bonusRandom': b.get('bonusRandom'), 'reset': b.get('reset'),
                    'minTopup': b.get('minTopup'), 'maxTopup': b.get('maxTopup'),
                    'referLink': b.get('referLink')
                })
        return len(bonuses)
