import json, hmac, hashlib, time, requests
from requests.auth import AuthBase
import base64
class CoinbaseWalletAuth(AuthBase):

    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        # message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        # hmac_key = base64.b64decode(self.secret_key)
        # signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        # signature_b64 = base64.b64encode(signature.digest()).decode()
        message = timestamp + request.method + request.path_url.split('?')[0] + str(request.body or '')
        signature = hmac.new(self.secret_key.encode('utf-8'), message.encode('utf-8'), digestmod=hashlib.sha256).digest()

        request.headers.update({
            'CB-ACCESS-SIGN': signature.hex(),
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            # 'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request