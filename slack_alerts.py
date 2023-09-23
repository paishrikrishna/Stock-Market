import requests
import json


class slack_alerts:

    def __init__(self,msg):
        self.msg = msg

    def send_slack_alert():
        self.url = 'https://hooks.slack.com/services/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'

        self.headers = {'Content-type': 'application/json'}

        self.data = {"text":self.msg}

        self.response = requests.post(self.url, headers=self.headers, data=json.dumps(self.data))

        print(self.response.status_code)
        print(self.response.text)



