import scrapy
from urllib.parse import urljoin

class WebCrawler(scrapy.Spider):
    name = "webcrawler"
    start_urls = ["https://www.arol.com/arol-canelli"]
    custom_settings = {
        "DEPTH_LIMIT": 1,
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "output.jsonl": {
                "format": "jsonlines",
                "encoding": "utf8",
                "overwrite": True
            }
        }
    }

    def __init__(self, *args, **kwargs):
        super(WebCrawler, self).__init__(*args, **kwargs)
        self.visited_urls = set()

    def parse(self, response):
        if response.url in self.visited_urls:
            return
        self.visited_urls.add(response.url)

        # Yield the raw HTML content
        yield {
            "url": response.url,
            "html": response.text
        }

        base_url = response.url.split("/")[2]
        for href in response.css("a::attr(href)").getall():
            try:
                absolute_url = urljoin(response.url, href)
                if base_url in absolute_url and absolute_url not in self.visited_urls and not any(
                        excluded in absolute_url for excluded in ["/it", "/fr"]):
                    yield response.follow(
                        absolute_url,
                        callback=self.parse,
                        errback=self.errback
                    )
            except Exception as e:
                self.logger.error(f"Error processing URL {href}: {str(e)}")

    def errback(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}")
        yield {
            "url": failure.request.url,
            "error": str(failure.value)
        }