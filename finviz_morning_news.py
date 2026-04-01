import argparse
import html
import re
import sys
import textwrap
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
FINVIZ_MARKET_NEWS_URL = "https://finviz.com/news.ashx"
STOPWORDS = {
    "a", "about", "after", "all", "an", "and", "are", "as", "at", "be", "been",
    "but", "by", "for", "from", "has", "have", "in", "into", "is", "it", "its",
    "more", "new", "not", "of", "on", "or", "our", "out", "s", "says", "that",
    "the", "their", "they", "this", "to", "up", "us", "was", "were", "will", "with",
}


@dataclass
class NewsItem:
    time_label: str
    headline: str
    url: str
    source: str


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


class FinvizMarketNewsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_news_block = False
        self.capture_link = False
        self.capture_time = False
        self.current_href = ""
        self.current_link_text: list[str] = []
        self.current_time_text: list[str] = []
        self.items: list[NewsItem] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)

        if tag == "a" and attr_map.get("href") == "news.ashx":
            self.in_news_block = True
            return

        if not self.in_news_block:
            return

        if tag == "a":
            href = attr_map.get("href", "") or ""
            if href and href != "news.ashx":
                self.capture_link = True
                self.current_href = href
                self.current_link_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.capture_link:
            headline = clean_text("".join(self.current_link_text))
            if headline and self.current_time_text:
                source = self.extract_source(self.current_href)
                time_label = clean_text("".join(self.current_time_text))
                self.items.append(
                    NewsItem(
                        time_label=time_label,
                        headline=headline,
                        url=self.current_href,
                        source=source,
                    )
                )
            self.capture_link = False
            self.current_href = ""
            self.current_link_text = []
            self.current_time_text = []

    def handle_data(self, data: str) -> None:
        if not self.in_news_block:
            return

        text = clean_text(data)
        if not text:
            return

        if self.capture_link:
            self.current_link_text.append(data)
        elif re.match(r"^(\d{1,2}:\d{2}[AP]M|[A-Z][a-z]{2}-\d{2})$", text):
            self.current_time_text.append(text)

    @staticmethod
    def extract_source(url: str) -> str:
        cleaned = re.sub(r"^https?://", "", url)
        return cleaned.split("/")[0]


def fetch_market_news(limit: int) -> list[NewsItem]:
    request = urllib.request.Request(
        FINVIZ_MARKET_NEWS_URL,
        headers={"User-Agent": USER_AGENT},
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            content = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Finviz returned HTTP {exc.code}.") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Unable to reach Finviz: {exc.reason}") from exc

    parser = FinvizMarketNewsParser()
    parser.feed(content)

    items = parser.items[:limit]
    if not items:
        raise RuntimeError("No market news items were parsed. Finviz may have changed its layout.")
    return items


def extract_topics(items: list[NewsItem], top_n: int) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for item in items:
        words = re.findall(r"[A-Za-z][A-Za-z'-]{2,}", item.headline.lower())
        counter.update(word for word in words if word not in STOPWORDS)
    return counter.most_common(top_n)


def build_summary(items: list[NewsItem]) -> list[str]:
    topics = extract_topics(items, top_n=8)
    summary: list[str] = []

    if topics:
        summary.append("Main themes in the headlines:")
        for topic, count in topics[:5]:
            summary.append(f"- {topic}: {count} mention(s)")

    macro_keywords = {
        "fed", "rates", "inflation", "treasury", "bond", "bonds", "yield", "economy",
        "oil", "crude", "jobs", "retail", "manufacturing", "mortgage", "recession",
        "futures", "stocks", "market", "volatility",
    }
    macro_hits = [item for item in items if any(word in item.headline.lower() for word in macro_keywords)]
    if macro_hits:
        summary.append("")
        summary.append("Macro and market-moving items:")
        for item in macro_hits[:5]:
            summary.append(f"- {item.time_label} | {item.headline}")

    return summary


def build_report(items: list[NewsItem], generated_at: datetime) -> str:
    lines: list[str] = []
    lines.append("FINVIZ MORNING MARKET NEWS REPORT")
    lines.append(f"Generated: {generated_at.strftime('%Y-%m-%d %I:%M %p %Z')}")
    lines.append("")

    lines.extend(build_summary(items))
    lines.append("")
    lines.append("Latest market headlines:")

    for item in items:
        wrapped = textwrap.fill(
            f"- {item.time_label} | {item.headline} ({item.source})",
            width=100,
            subsequent_indent="  ",
        )
        lines.append(wrapped)
        lines.append(f"  Link: {item.url}")

    lines.append("")
    lines.append("Note: This is a market headline digest from Finviz, not investment advice.")
    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a morning market news report from Finviz."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of market headlines to include (default: 20).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to save the report as a text file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        items = fetch_market_news(limit=args.limit)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    report = build_report(items, datetime.now().astimezone())
    print(report)

    if args.output:
        args.output.write_text(report, encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
