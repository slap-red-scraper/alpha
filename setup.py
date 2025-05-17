import configparser, requests, re, os, sys

class Setup:
    API_PATH = "/api/v1/index.php"

    def __init__(self, path='config.ini'):
        self.config = configparser.ConfigParser()
        if not os.path.exists(path):
            sys.exit(f"Missing config file: {path}")
        self.config.read(path)

    def get_credentials(self):
        try:
            return self.config['credentials']['mobile'], self.config['credentials']['password']
        except KeyError:
            sys.exit("Missing 'mobile' or 'password' in [credentials] section.")

    def get_url_file(self):
        try:
            return self.config['settings']['file']
        except KeyError:
            sys.exit("Missing 'file' setting in [settings] section.")

    def downline_enabled(self):
        return self.config['settings'].getboolean('downline', fallback=False)

    @staticmethod
    def clean_url(url):
        return re.sub(r'/\w+$', '', url)

    @staticmethod
    def extract_merchant_info(html):
        match = re.search(r'var MERCHANTID = (\d+);\nvar MERCHANTNAME = "(.*?)";', html)
        return match.groups() if match else (None, None)

    def login(self, url, mobile, password):
        try:
            html = requests.get(url).text
        except Exception as e:
            return None
        merchantID, merchantName = self.extract_merchant_info(html)
        if not merchantID:
            return None
        api_url = url + self.API_PATH
        payload = {
            'module': '/users/login',
            'mobile': mobile,
            'password': password,
            'merchantId': merchantID,
            'domainId': '0',
            'accessId': '',
            'accessToken': '',
            'walletIsAdmin': ''
        }
        try:
            resp = requests.post(api_url, data=payload).json()
        except Exception as e:
            return None
        data = resp.get('data', {})
        return {
            'merchantID': merchantID,
            'merchantName': merchantName,
            'access_id': data.get('id'),
            'token': data.get('token'),
            'api_url': api_url
        } if data.get('token') else None
