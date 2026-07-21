import paramiko
import platform
import random
import stat
import time
import requests
import socket
import logging
import io
import json
import os
import shutil
import tempfile
import zipfile
from selenium import webdriver
import atexit


CHROMEDRIVER_FEED = ('https://googlechromelabs.github.io/chrome-for-testing/'
                     'latest-patch-versions-per-build-with-downloads.json')
CHROMEDRIVER_CACHE = os.environ.get('CHROMEDRIVER_CACHE',
                                    os.path.expanduser('~/.cache/chromedriver'))
CHROMEDRIVER_FALLBACK = '/usr/local/bin/chromedriver'


def _chromedriver_platform():
    system, machine = platform.system(), platform.machine()
    if system == 'Darwin':
        return 'mac-arm64' if machine == 'arm64' else 'mac-x64'
    return 'linux64'


def resolve_chromedriver(browser_version, fallback=CHROMEDRIVER_FALLBACK):
    """Fetch a chromedriver matching the Chrome inside the container.

    chromedriver only drives a browser of its own major version, and the
    browser lives in a separately built image (google-chrome-in-a-box) which
    updates on its own schedule -- so a chromedriver baked in at build time
    goes stale the moment either image is rebuilt. Resolving against the
    version the container actually reports keeps the two in step.
    """
    try:
        build = '.'.join(browser_version.split('.')[:3])
        entry = requests.get(CHROMEDRIVER_FEED, timeout=60).json()['builds'][build]
        target = os.path.join(CHROMEDRIVER_CACHE, entry['version'], 'chromedriver')
        if not os.path.exists(target):
            url = next(d['url'] for d in entry['downloads']['chromedriver']
                       if d['platform'] == _chromedriver_platform())
            archive = zipfile.ZipFile(io.BytesIO(requests.get(url, timeout=300).content))
            member = next(n for n in archive.namelist() if n.endswith('/chromedriver'))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with archive.open(member) as src, open(target, 'wb') as out:
                shutil.copyfileobj(src, out)
            os.chmod(target, os.stat(target).st_mode | stat.S_IEXEC)
        logging.info('USING CHROMEDRIVER %s for Chrome %s', entry['version'], browser_version)
        return target
    except Exception:
        logging.exception('Failed to resolve a chromedriver for Chrome %s, falling back to %s',
                          browser_version, fallback)
        return fallback


class google_chrome_driver():

    def __init__(self, wait=True, initial='https://data.gov.il', chromedriver=None):
        if wait:
            time.sleep(random.randint(1, 600))
        self.hostname = 'tzabar.obudget.org'
        self.hostname_ip = socket.gethostbyname(self.hostname)
        username = 'adam'
        self.port = random.randint(20000, 30000)
        # print('Creating connection for client #{}'.format(self.port))
        self.docker_container = None

        atexit.register(self.teardown)

        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client._policy = paramiko.WarningPolicy()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.client.connect(username=username, hostname=self.hostname)

        count_cmd = 'docker ps'
        while wait:
            stdin, stdout, stderr = self.client.exec_command(count_cmd)
            containers = stdout.read().decode('utf8').split('\n')
            running = len([x for x in containers if 'google-chrome' in x])
            if running < 6:
                break
            logging.info('COUNTED %d running containers, waiting', running)
            time.sleep(60)

        cmd = f'docker run -p {self.port}:{self.port} -p {self.port+1}:{self.port+1} --add-host stats.tehila.gov.il:127.0.0.1 -d ghcr.io/whiletrue-industries/google-chrome-in-a-box {self.port} {self.port+1} {initial}'
        stdin, stdout, stderr = self.client.exec_command(cmd)

        while not self.docker_container:
            time.sleep(3)
            self.docker_container = stdout.read().decode('utf8').strip()

        time.sleep(45)
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

            if chromedriver is None:
                version = requests.get(f'http://{self.hostname_ip}:{self.port}/json/version').json()
                chromedriver = resolve_chromedriver(version['Browser'].split('/')[-1])

            chrome_options = webdriver.ChromeOptions()
            # By IP, not hostname: Chrome rejects devtools requests whose Host
            # header is neither localhost nor an IP address.
            chrome_options.debugger_address = f'{self.hostname_ip}:{self.port}'
            self.driver = webdriver.Chrome(chromedriver, options=chrome_options)
        except Exception:
            logging.exception('Error in setting up')
            self.teardown()

    def teardown(self):
        # print('Closing connection for client #{}'.format(self.port))
        if self.docker_container:
            try:
                stdin, stdout, stderr = self.client.exec_command(f'docker stop {self.docker_container}')
                stdout.read()
            except:
                logging.warning('FAILED to teardown google-chrome')
        try:
            self.client.close()
        except:
            logging.warning('FAILED to close connection')

    def json(self, url):
        self.driver.get(url)
        time.sleep(1)
        return json.loads(self.driver.find_element_by_css_selector('body > pre').text)

    def html(self, url):
        self.driver.get(url)
        time.sleep(1)
        return self.driver.execute_script("return document.documentElement.outerHTML;")

    def curl(self, url, outfile):
        _, stdout, stderr = self.client.exec_command(f'docker exec {self.docker_container} curl "{url}" -o /downloads/{outfile}')

    def list_downloads(self):
        cmd = f'docker exec {self.docker_container} ls -la /downloads/'
        _, stdout, _ = self.client.exec_command(cmd)
        logging.info('CURRENT DOWNLOADS:\n%s', stdout.read().decode('utf8'))
        cmd = f'docker exec {self.docker_container} ls -1 /downloads/'
        _, stdout, _ = self.client.exec_command(cmd)
        return stdout.read().decode('utf8').split('\n')

    def download(self, url, any_file=False, format='', use_curl=False, outfile='downloaded_file'):
        expected = None
        if outfile:
            expected = outfile
        elif not any_file:
            expected = os.path.basename(url)
            expected = expected.split('?')[0]
        logging.info('EXPECTING: %r', expected)
        for attempt in range(3):
            logging.info('Attempt %d', attempt)
            current_downloads = self.list_downloads()
            downloads = []
            timeout = 0
            downloading = False
            already_downloaded = expected and expected not in current_downloads
            if not already_downloaded:
                if use_curl:
                    self.curl(url, outfile)
                else:
                    self.driver.get(url)
            while True:
                if not already_downloaded:
                    time.sleep(3 if use_curl else 60)
                    timeout += 1

                downloads = self.list_downloads()
                logging.info('DOWNLOADS: %r', downloads)
                downloading = any('crdownload' in download for download in downloads)
                if expected is None and len(downloads) > len(current_downloads):
                    diff = set(downloads) - set(current_downloads)
                    while len(diff) > 0:
                        candidate = diff.pop()
                        if 'crdownload' not in candidate:
                            expected = candidate
                            logging.info('GOT FILENAME: %s', expected)
                            break

                if expected in downloads:
                    logging.info('found {} in {}'.format(expected, downloads))
                    time.sleep(3 if use_curl else 20)
                    out = tempfile.NamedTemporaryFile(delete=False, suffix=expected + format)
                    url = f'http://{self.hostname}:{self.port+1}/{expected}'
                    response = requests.get(url, stream=True, timeout=30)
                    assert response.status_code == 200
                    stream = response.raw
                    shutil.copyfileobj(stream, out)
                    out.close()
                    logging.info('DELETE %s', requests.delete(url).text)
                    return out.name

                if not downloading:
                    logging.info('TIMED OUT while NOT downloading')
                    break

                # if timeout > 30 and downloading:
                #     logging.info('TIMED OUT while downloading')
                #     break

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
