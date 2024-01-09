import time
from copy import deepcopy
from functools import partial
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any, MutableMapping, Optional, Union
from urllib.request import ProxyHandler, Request, build_opener, urlopen

import requests
from requests.cookies import RequestsCookieJar, cookiejar_from_dict, get_cookie_header
from rich import filesize

from devine.core.constants import DOWNLOAD_CANCELLED

MAX_ATTEMPTS = 5
RETRY_WAIT = 2


def urllib(
    uri: Union[str, list[str]],
    out: Path,
    headers: Optional[dict] = None,
    cookies: Optional[Union[MutableMapping[str, str], RequestsCookieJar]] = None,
    proxy: Optional[str] = None,
    progress: Optional[partial] = None,
    *_: Any,
    **__: Any
) -> int:
    """
    Download files using Python urllib.
    https://docs.python.org/3/library/urllib.html

    If multiple URLs are provided they will be downloaded in the provided order
    to the output directory. They will not be merged together.
    """
    if isinstance(uri, list) and len(uri) == 1:
        uri = uri[0]

    if isinstance(uri, list):
        if out.is_file():
            raise ValueError("Expecting out to be a Directory path not a File as multiple URLs were provided")
        uri = [
            (url, out / f"{i:08}.mp4")
            for i, url in enumerate(uri)
        ]
    else:
        uri = [(uri, out.parent / out.name)]

    headers = headers or {}
    headers = {
        k: v
        for k, v in headers.items()
        if k.lower() != "accept-encoding"
    }

    if cookies and not isinstance(cookies, CookieJar):
        cookies = cookiejar_from_dict(cookies)

    if proxy:
        proxy_handler = ProxyHandler({
            proxy.split("://")[0]: proxy
        })
        opener = build_opener(proxy_handler)
    else:
        opener = urlopen

    if progress:
        progress(total=len(uri))

    download_sizes = []
    last_speed_refresh = time.time()

    for url, out_path in uri:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        headers_ = deepcopy(headers)

        if cookies:
            mock_request = requests.Request(url=url)
            cookie_header = get_cookie_header(cookies, mock_request)
            if cookie_header:
                headers_["Cookie"] = cookie_header

        attempts = 1

        while True:
            try:
                req = Request(
                    url=url,
                    headers=headers_
                )

                res = opener.open(req)

                cookies.extract_cookies(res, req)

                with open(out_path, "wb") as f:
                    written = 0
                    while True:
                        chunk = res.read(1024)
                        if not chunk:
                            break
                        download_size = len(chunk)
                        f.write(chunk)
                        written += download_size
                        if progress:
                            progress(advance=1)

                            now = time.time()
                            time_since = now - last_speed_refresh

                            download_sizes.append(download_size)
                            if time_since > 5 or download_size < 1024:
                                data_size = sum(download_sizes)
                                download_speed = data_size / (time_since or 1)
                                progress(downloaded=f"{filesize.decimal(download_speed)}/s")
                                last_speed_refresh = now
                                download_sizes.clear()
                break
            except Exception as e:
                if DOWNLOAD_CANCELLED.is_set() or attempts == MAX_ATTEMPTS:
                    raise e
                time.sleep(RETRY_WAIT)
                attempts += 1

    return 0


__all__ = ("urllib",)
