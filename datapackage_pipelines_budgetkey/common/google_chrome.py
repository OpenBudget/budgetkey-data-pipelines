import paramiko
import random
import time
import requests
import socket
import logging
import json
import os
import shutil
import tempfile
from selenium import webdriver
import atexit


class google_chrome_driver():

    def __init__(self, wait=True):
        if wait:
            time.sleep(random.randint(1, 300))
        self.hostname = 'tzabar.obudget.org'
        self.hostname_ip = socket.gethostbyname(self.hostname)
        username = 'adam'
        self.port = random.randint(20000, 30000)
        # print('Creating connection for client #{}'.format(self.port))

        atexit.register(self.teardown)

        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client._policy = paramiko.WarningPolicy()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.client.connect(username=username, hostname=self.hostname)

        count_cmd = 'docker ps'
        while wait:
            stdin, stdout, stderr = self.client.exec_command(count_cmd)
            containers = stdout.read().decode('ascii').split('\n')
            running = len([x for x in containers if 'google-chrome' in x])
            if running < 6:
                break
            logging.info('COUNTED %d running containers, waiting', running)
            time.sleep(60)

        cmd = f'docker run -p {self.port}:{self.port} -p {self.port+1}:{self.port+1} -d akariv/google-chrome-in-a-box {self.port} {self.port+1}'
        stdin, stdout, stderr = self.client.exec_command(cmd)

        self.docker_container = None
        while not self.docker_container:
            time.sleep(3)
            self.docker_container = stdout.read().decode('ascii').strip()

        try:
            windows = None
            for i in range(10):
                time.sleep(6)
                try:
                    windows = requests.get(f'http://{self.hostname_ip}:{self.port}/json/list').json()
                    if len(windows) == 1:
                        break
                    logging.info('GOT %d WINDOWS: %r', len(windows), windows)
                except Exception as e:
                    logging.error('Waiting %s (%s): %s', i, windows, e)

            chrome_options = webdriver.ChromeOptions()
            chrome_options.debugger_address = f'{self.hostname_ip}:{self.port}'
            self.driver = webdriver.Chrome('/usr/local/bin/chromedriver', options=chrome_options)
        except Exception:
            logging.exception('Error in setting up')
            self.teardown()

    def teardown(self):
        # print('Closing connection for client #{}'.format(self.port))
        if self.docker_container:
            stdin, stdout, stderr = self.client.exec_command(f'docker stop {self.docker_container}')
            stdout.read()
        self.client.close()

    def json(self, url):
        self.driver.get(url)
        time.sleep(1)
        return json.loads(self.driver.find_element_by_css_selector('body > pre').text)

    def list_downloads(self):
        cmd = f'docker exec {self.docker_container} ls -1 /downloads/'
        _, stdout, _ = self.client.exec_command(cmd)
        return stdout.read().decode('utf8').split('\n')

    def download(self, url, any_file=False, format=''):
        expected = None
        if not any_file:
            expected = os.path.basename(url)
        current_downloads = self.list_downloads()
        logging.info('CURRENT DOWNLOADS: %r', current_downloads)
        for j in range(3):
            print('Attempt {}'.format(j))
            self.driver.get(url)
            downloads = []
            for i in range(10):
                time.sleep(6)
                downloads = self.list_downloads()
                if expected is None and len(downloads) > len(current_downloads):
                    expected = (set(downloads) - set(current_downloads)).pop()
                    logging.info('GOT FILNAME: %s', expected)

                if expected in downloads:
                    print('found {} in {}'.format(expected, downloads))
                    time.sleep(20)
                    out = tempfile.NamedTemporaryFile(delete=False, suffix=expected + format)
                    url = f'http://{self.hostname}:{self.port+1}/{expected}'
                    stream = requests.get(url, stream=True, timeout=30).raw
                    shutil.copyfileobj(stream, out)
                    out.close()
                    return out.name
        assert False, 'Failed to download file, %r' % downloads


def finalize(f):
    def func(package):
        yield package.pkg
        yield from package
        try:
            logging.warning('Finalizing connection')
            f()
        except Exception:
            logging.exception('Failed to finalize')
    return func



if __name__ == '__main__':
    gcd = google_chrome_driver()
    c = gcd.driver
    c.get('http://data.gov.il/api/action/package_search')
    time.sleep(2)
    print(c.find_element_by_css_selector('body > pre').text)
    gcd.teardown()
