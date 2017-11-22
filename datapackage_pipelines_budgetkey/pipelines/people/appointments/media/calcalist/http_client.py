import requests


class HttpClient(object):

    @staticmethod
    def download_page_source(url):
        response = requests.get(url)
        if HttpClient._is_successful_response(response):
            return response.text
        else:
            raise RuntimeError('URL: %s returned a failed response. Response status: %d' % (url, response.status_code))

    @staticmethod
    def _is_successful_response(response):
        return 200 <= response.status_code < 300
