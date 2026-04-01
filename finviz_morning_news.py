import argparse
import html
import json
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
DEFAULT_LM_STUDIO_URL = "http://127.0.0.1:1234/v1/chat/completions"
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


@dataclass
class ArticleContext:
    headline: str
    url: str
    source: str
    text: str


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


class ArticleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_paragraph = False
        self.skip_depth = 0
        self.paragraph_buffer: list[str] = []
        self.paragraphs: list[str] = []
        self.meta_description = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs}
        if tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
            return

        if tag == "meta":
            name = (attr_map.get("name") or attr_map.get("property") or "").lower()
            if name in {"description", "og:description", "twitter:description"}:
                content = clean_text(attr_map.get("content") or "")
                if content and not self.meta_description:
                    self.meta_description = content

        if self.skip_depth == 0 and tag == "p":
            self.in_paragraph = True
            self.paragraph_buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"} and self.skip_depth > 0:
            self.skip_depth -= 1
            return

        if tag == "p" and self.in_paragraph:
            paragraph = clean_text("".join(self.paragraph_buffer))
            if len(paragraph) >= 80:
                self.paragraphs.append(paragraph)
            self.in_paragraph = False
            self.paragraph_buffer = []

    def handle_data(self, data: str) -> None:
        if self.skip_depth > 0:
            return
        if self.in_paragraph:
            self.paragraph_buffer.append(data)


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


def fetch_article_context(
    items: list[NewsItem],
    article_limit: int,
    max_chars_per_article: int,
) -> list[ArticleContext]:
    contexts: list[ArticleContext] = []

    for item in items[:article_limit]:
        request = urllib.request.Request(item.url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                content_type = response.headers.get("Content-Type", "")
                if "html" not in content_type.lower():
                    continue
                html_text = response.read().decode("utf-8", errors="replace")
        except Exception:
            continue

        parser = ArticleTextParser()
        parser.feed(html_text)

        sections: list[str] = []
        if parser.meta_description:
            sections.append(f"Summary: {parser.meta_description}")
        if parser.paragraphs:
            sections.append(" ".join(parser.paragraphs[:6]))

        article_text = clean_text(" ".join(sections))
        if not article_text:
            continue

        contexts.append(
            ArticleContext(
                headline=item.headline,
                url=item.url,
                source=item.source,
                text=article_text[:max_chars_per_article],
            )
        )

    return contexts


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


def build_llm_prompt(items: list[NewsItem], article_contexts: list[ArticleContext] | None = None) -> str:
    headlines = "\n".join(
        f"- {item.time_label} | {item.headline} ({item.source})"
        for item in items
    )
    article_section = ""
    if article_contexts:
        article_chunks = []
        for article in article_contexts:
            article_chunks.append(
                f"Headline: {article.headline}\n"
                f"Source: {article.source}\n"
                f"URL: {article.url}\n"
                f"Article Notes: {article.text}"
            )
        article_section = "\n\nArticle extracts:\n" + "\n\n".join(article_chunks)

    return (
        "You are a financial news assistant creating a concise morning market brief.\n"
        "Using only the provided material below, write:\n"
        "1. A 3-5 sentence market overview.\n"
        "2. Three bullet points for key catalysts.\n"
        "3. Two bullet points for risks or uncertainty.\n"
        "Do not mention individual stock tickers unless the headline is clearly market-moving.\n"
        "Do not invent facts beyond the provided headlines and article extracts.\n\n"
        f"Headlines:\n{headlines}{article_section}"
    )


def generate_lm_studio_summary(
    items: list[NewsItem],
    article_contexts: list[ArticleContext] | None,
    model: str,
    endpoint: str,
    temperature: float,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                "You summarize financial headlines into a short, readable market briefing. "
                    "Stay grounded in the provided material and avoid speculation."
                ),
            },
            {"role": "user", "content": build_llm_prompt(items, article_contexts)},
        ],
        "temperature": temperature,
        "max_tokens": 500,
    }

    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer lm-studio",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LM Studio returned HTTP {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            "Unable to reach LM Studio. Make sure the local server is running and the "
            f"endpoint is correct: {endpoint}"
        ) from exc

    try:
        parsed = json.loads(body)
        return parsed["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        raise RuntimeError("LM Studio response could not be parsed.") from exc


def build_report(items: list[NewsItem], generated_at: datetime, ai_summary: str | None = None) -> str:
    lines: list[str] = []
    lines.append("FINVIZ MORNING MARKET NEWS REPORT")
    lines.append(f"Generated: {generated_at.strftime('%Y-%m-%d %I:%M %p %Z')}")
    lines.append("")

    if ai_summary:
        lines.append("AI market brief:")
        lines.append(ai_summary.strip())
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
    parser.add_argument(
        "--use-lm-studio",
        action="store_true",
        help="Generate an AI summary with LM Studio's local server.",
    )
    parser.add_argument(
        "--lm-studio-endpoint",
        default=DEFAULT_LM_STUDIO_URL,
        help="LM Studio chat completions endpoint.",
    )
    parser.add_argument(
        "--lm-studio-model",
        default="local-model",
        help="Model name exposed by LM Studio.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.2,
        help="Sampling temperature for the LM Studio summary (default: 0.2).",
    )
    parser.add_argument(
        "--read-articles",
        action="store_true",
        help="Fetch linked articles and include extracted article text in the AI summary prompt.",
    )
    parser.add_argument(
        "--article-limit",
        type=int,
        default=5,
        help="Number of linked articles to read when --read-articles is enabled (default: 5).",
    )
    parser.add_argument(
        "--max-article-chars",
        type=int,
        default=2000,
        help="Maximum characters to keep per article extract (default: 2000).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        items = fetch_market_news(limit=args.limit)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    ai_summary = None
    if args.use_lm_studio:
        article_contexts = None
        if args.read_articles:
            article_contexts = fetch_article_context(
                items=items,
                article_limit=args.article_limit,
                max_chars_per_article=args.max_article_chars,
            )
        try:
            ai_summary = generate_lm_studio_summary(
                items=items,
                article_contexts=article_contexts,
                    model=args.lm_studio_model,
                endpoint=args.lm_studio_endpoint,
                temperature=args.temperature,
            )
        except Exception as exc:
            print(f"Warning: {exc}", file=sys.stderr)

    report = build_report(items, datetime.now().astimezone(), ai_summary=ai_summary)
    print(report)

    if args.output:
        args.output.write_text(report, encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
