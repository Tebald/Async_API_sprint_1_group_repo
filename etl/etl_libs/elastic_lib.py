import json
import logging
from http import HTTPStatus

import backoff
import requests
from requests import Request


class Elastic:
    """ Class to handle all methods related to ElasticSearch communication. """

    def __init__(self, url: str):
        self.url = url
        self.session = requests.Session()

    @staticmethod
    def make_curl(request: Request) -> str:
        """ Makes str representation of the request for debugging. """

        command = "curl -vX {method} '{uri}' -H {headers} -d '{data}'"
        method = request.method
        uri = request.url

        data = request.data
        headers = ['"{0}: {1}"'.format(k, v) for k, v in request.headers.items()]
        headers = " -H ".join(headers)
        return command.format(method=method, headers=headers, data=data, uri=uri)

    @staticmethod
    def check_index(host: str, port: int, index_name: str) -> dict:
        """
        Creates Elastic index.
        :param host:
        :param port:
        :param index_name:
        :return:
        """
        https_client = Elastic(url=f'http://{host}:{port}/{index_name}')
        result = https_client.get(simulate=False)
        return result

    @backoff.on_exception(backoff.expo,
                          (
                                  requests.exceptions.ConnectionError,
                                  requests.exceptions.HTTPError,
                                  requests.exceptions.Timeout
                          ),
                          max_time=300,
                          max_tries=10)
    def get(self, simulate: bool = False, timeout: int = 30) -> dict:
        """
        Send GET request.
        :param simulate:
        :param timeout:
        :return:
        """

        prepared_request = requests.Request(method='GET', url=self.url)

        if simulate:
            logging.info(Elastic.make_curl(prepared_request))
            return {}

        response = self.session.send(prepared_request.prepare(), timeout=timeout)
        if response.status_code == HTTPStatus.OK:
            return {
                'status_code': response.status_code,
                'text': response.text
            }

        return {
            'status_code': response.status_code,
            'errors': json.loads(response.text)['error']['root_cause'],
            'text': response.text
        }

    @backoff.on_exception(backoff.expo,
                          (
                                  requests.exceptions.ConnectionError,
                                  requests.exceptions.HTTPError,
                                  requests.exceptions.Timeout
                          ),
                          max_time=300,
                          max_tries=10)
    def post(self, headers: dict, data: str, simulate: bool = False, timeout: int = 30) -> dict:
        """
        Send POST request.
        :param headers:
        :param data:
        :param simulate:
        :param timeout:
        :return:
        """

        prepared_request = requests.Request(method='POST', url=self.url, data=data, headers=headers)

        if simulate:
            logging.info(Elastic.make_curl(prepared_request))
            return {}

        response = self.session.send(prepared_request.prepare(), timeout=timeout)
        if response.status_code == HTTPStatus.OK:
            return {
                'status_code': response.status_code,
                'errors': json.loads(response.text)['errors'],
                'text': response.text
            }

        return {
            'status_code': response.status_code,
            'errors': json.loads(response.text)['errors'],
            'text': response.text
        }

    @backoff.on_exception(backoff.expo,
                          (
                                  requests.exceptions.ConnectionError,
                                  requests.exceptions.HTTPError,
                                  requests.exceptions.Timeout
                          ),
                          max_time=300,
                          max_tries=10)
    def put(self, headers: dict, data: str, simulate: bool = False, timeout: int = 30) -> dict:
        """
        Send PUT request.
        :param headers:
        :param data:
        :param simulate:
        :param timeout:
        :return:
        """

        prepared_request = requests.Request(method='PUT', url=self.url, data=data, headers=headers)

        if simulate:
            logging.info(Elastic.make_curl(prepared_request))
            return {}

        response = self.session.send(prepared_request.prepare(), timeout=timeout)
        if response.status_code == HTTPStatus.OK:
            return {
                'status_code': response.status_code,
                'text': response.text
            }

        return {
            'status_code': response.status_code,
            'errors': json.loads(response.text)['error']['root_cause']
        }
