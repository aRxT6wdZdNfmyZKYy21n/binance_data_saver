import codecs
import os
import os.path


class ProxyUtils:
    @staticmethod
    def get_socks5_proxy_urls(
        socks5_proxies_file_names: list[str],
    ) -> list[str | None]:
        socks5_proxy_urls: list[str | None] = []

        for socks5_proxies_file_name in socks5_proxies_file_names:
            with codecs.open(
                os.path.join(
                    'data/',
                    socks5_proxies_file_name,
                ),
                'r',
                'utf-8',
            ) as proxies_file:
                socks5_proxy_urls.extend(
                    filter(
                        bool,
                        map(
                            str.strip,
                            proxies_file,
                        ),
                    ),
                )

        # socks5_proxy_urls.append(
        #     None
        # )

        return socks5_proxy_urls
