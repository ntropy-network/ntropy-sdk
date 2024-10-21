import logging
import time
import uuid
from json import JSONDecodeError
from typing import Dict, Optional

import requests

from ntropy_sdk import VERSION
from ntropy_sdk.v2.errors import error_from_http_status_code, NtropyError


class HttpClient:
    def __init__(self, session: Optional[requests.Session] = None):
        self._session = session

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
            self._session.mount("https://", TCPKeepAliveAdapter())
        return self._session

    @property
    def session(self) -> requests.Session:
        return self._get_session()

    @session.setter
    def session(self, session: requests.Session):
        self._session = session

    def retry_ratelimited_request(
        self,
        *,
        method: str,
        url: str,
        params: Optional[Dict[str, str]] = None,
        payload: Optional[object] = None,
        payload_json_str: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        log_level=logging.DEBUG,
        request_id: Optional[str] = None,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        retries: int = 1,
        timeout: int = 10 * 60,
        retry_on_unhandled_exception: bool = False,
        extra_headers: Optional[dict] = None,
        **request_kwargs,
    ):
        """Executes a request to an endpoint in the Ntropy API (given the `base_url` parameter).
        Catches expected errors and wraps them in NtropyError.
        Retries the request for Rate-Limiting errors or Unexpected Errors (50x)


        Raises
        ------
        NtropyError
            If the request failed after the maximum number of retries.
        """

        if payload_json_str is not None and payload is not None:
            raise ValueError(
                "payload_json_str and payload cannot be used simultaneously"
            )

        if request_id is None:
            request_id = uuid.uuid4().hex
        cur_session = session
        if cur_session is None:
            cur_session = self._get_session()

        headers = {
            "User-Agent": f"ntropy-sdk/{VERSION}",
            "X-Request-ID": request_id,
        }
        if api_key is not None:
            headers["X-API-Key"] = api_key
        if payload_json_str is None:
            request_kwargs["json"] = payload
        else:
            headers["Content-Type"] = "application/json"
            request_kwargs["data"] = payload_json_str
        if extra_headers:
            headers.update(extra_headers)

        backoff = 1
        for _ in range(retries):
            try:
                resp = cur_session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    **request_kwargs,
                )
            except requests.ConnectionError:
                # Rebuild session on connection error and retry
                if session is None:
                    self._session = None
                    cur_session = self._get_session()
                    continue
                else:
                    raise

            if resp.status_code == 429:
                try:
                    retry_after = int(resp.headers.get("retry-after", "1"))
                except ValueError:
                    retry_after = 1
                if retry_after <= 0:
                    retry_after = 1

                if logger:
                    logger.log(
                        log_level,
                        "Retrying in %s seconds due to ratelimit",
                        retry_after,
                    )
                time.sleep(retry_after)

                continue
            elif resp.status_code == 503:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)

                if logger:
                    logger.log(
                        log_level,
                        "Retrying in %s seconds due to unavailability in the server side",
                        backoff,
                    )
                continue

            elif (
                resp.status_code >= 500 and resp.status_code <= 511
            ) and retry_on_unhandled_exception:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)

                if logger:
                    logger.log(
                        log_level,
                        "Retrying in %s seconds due to unhandled exception in the server side",
                        backoff,
                    )

                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                status_code = e.response.status_code

                try:
                    content = e.response.json()
                except JSONDecodeError:
                    content = {}

                err = error_from_http_status_code(request_id, status_code, content)
                raise err
            return resp
        raise NtropyError(f"Failed to {method} {url} after {retries} attempts")
