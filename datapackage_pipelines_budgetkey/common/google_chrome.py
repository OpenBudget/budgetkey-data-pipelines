import paramiko
import random
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import requests
import socket


class google_chrome_driver():

    def __init__(self):
        hostname = 'tzabar.obudget.org'
        hostname_ip = socket.gethostbyname(hostname)
        username = 'adam'
        port = random.randint(20000, 30000)
        cmd = f'docker run -p {port}:{port} -d akariv/google-chrome-in-a-box {port}'

        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client._policy = paramiko.WarningPolicy()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.client.connect(username=username, hostname=hostname)
        stdin, stdout, stderr = self.client.exec_command(cmd)

        time.sleep(10)
        self.docker_container = stdout.read().decode('ascii').strip()

        assert len(requests.get(f'http://{hostname_ip}:{port}/json/list').json()) > 0

        chrome_options = webdriver.ChromeOptions()
        chrome_options.debugger_address = f'{hostname_ip}:{port}'
        self.driver = webdriver.Chrome('/usr/local/bin/chromedriver', options=chrome_options)

    def teardown(self):
        if self.docker_container:
            stdin, stdout, stderr = self.client.exec_command(f'docker stop {self.docker_container}')
            stdout.read()
        self.client.close()


if __name__ == '__main__':
    gcd = google_chrome_driver()
    c = gcd.driver
    c.get('http://data.gov.il/api/action/package_search')
    time.sleep(2)
    print(c.find_element_by_css_selector('body > pre').text)
    gcd.teardown()
