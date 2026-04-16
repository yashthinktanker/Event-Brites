import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlunparse

from scrapling.fetchers import StealthyFetcher


BASE_URL = "https://www.eventbrite.com/d/az--phoenix/all-events/"
EVENT_DETAIL_WORKERS = 4

SKIP_URLS_FILE = Path(__file__).resolve().parent / "skip_urls.txt"

if not SKIP_URLS_FILE.exists():
    raise FileNotFoundError(f"{SKIP_URLS_FILE} not found in project root")
FIELDNAMES = [
    "Event Name",
    "Event Description",
    "Event Image",
    "Event Date",
    "Event Time",
    "Event End Date",
    "Event End Time",
    "Place",
    "Street",
    "City",
    "State",
    "Pincode",
    "Event URL",
]


def canonical_event_url(value):
    value = re.sub(r"\s+", " ", value or "").strip().split("#")[0]
    if not value:
        return ""
    if value.lower().startswith(("www.eventbrite.", "eventbrite.")):
        value = f"https://{value}"

    parsed = urlparse(normalize_eventbrite_url(value))
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    path = unquote(parsed.path).rstrip("/").lower()
    return f"{host}{path}"


def normalize_eventbrite_url(value):
    value = re.sub(r"\s+", " ", value or "").strip().split("#")[0]
    if not value:
        return ""

    if value.lower().startswith(("www.eventbrite.", "eventbrite.")):
        value = f"https://{value}"

    parsed = urlparse(urljoin("https://www.eventbrite.com", value))
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if host in {"eventbrite.sg", "eventbrite.ca"}:
        host = "eventbrite.com"

    normalized_path = unquote(parsed.path)
    return urlunparse((
        parsed.scheme or "https",
        host,
        normalized_path,
        parsed.params,
        parsed.query,
        "",
    ))


def load_skip_urls(path):
    try:
        with open(path, "r", encoding="utf-8") as skip_file:
            return {
                canonical_event_url(line.strip().split("?")[0])
                for line in skip_file
                if line.strip() and not line.strip().startswith("#")
            }
    except FileNotFoundError:
        return set()


SKIP_URLS = load_skip_urls(SKIP_URLS_FILE)


def clean_date(text, year=2026):
    if not text:
        return ""

    text = text.strip().lower()
    text = re.sub(r"(st|nd|rd|th)", "", text)
    text = text.replace(",", " ")
    text = re.sub(r"\s+", " ", text).strip()

    match = re.search(r"([a-zA-Z]+)\s+(\d{1,2})", text)
    if match:
        month, day = match.groups()
        raw = f"{month} {day} {year}"
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%m/%d/%Y")
            except ValueError:
                continue

    match = re.search(r"(\d{1,2})\s+([a-zA-Z]+)", text)
    if match:
        day, month = match.groups()
        raw = f"{month} {day} {year}"
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%m/%d/%Y")
            except ValueError:
                continue

    match = re.search(r"(\d{1,2})[/-](\d{1,2})$", text)
    if match:
        month, day = match.groups()
        try:
            return datetime(year, int(month), int(day)).strftime("%m/%d/%Y")
        except ValueError:
            return ""

    return ""


def extract_event_schedule(text):
    cleaned_text = clean_text(text)
    if not cleaned_text:
        return "", "", "", ""

    date_matches = [clean_date(match.group(0)) for match in re.finditer(
        r"([A-Za-z]+(?:\s+\w+)?\s+\d{1,2}|\d{1,2}\s+[A-Za-z]+|\d{1,2}[/-]\d{1,2})",
        cleaned_text,
    )]
    date_matches = [value for value in date_matches if value]

    time_matches = [
        format_time(match.group(1))
        for match in re.finditer(r"(\d{1,2}(?::\d{2})?\s*[ap]m)", cleaned_text, re.IGNORECASE)
    ]
    time_matches = [value for value in time_matches if value]

    start_date = date_matches[0] if date_matches else ""
    end_date = date_matches[1] if len(date_matches) > 1 else start_date
    start_time = time_matches[0] if time_matches else ""
    end_time = time_matches[1] if len(time_matches) > 1 else ""

    return start_date, start_time, end_date, end_time


def format_time(raw_time):
    if not raw_time:
        return ""

    raw_time = re.sub(r"gmt.*", "", raw_time.lower()).strip()
    raw_time = raw_time.replace(".", "")

    for fmt in ("%I %p", "%I:%M %p", "%I%p", "%I:%M%p"):
        try:
            return datetime.strptime(raw_time, fmt).strftime("%I:%M %p")
        except ValueError:
            continue

    return ""


def clean_image_url(url):
    if not url:
        return ""

    absolute_url = urljoin("https://www.eventbrite.com", url)

    if "_next/image" in absolute_url:
        parsed = urlparse(absolute_url)
        query = parse_qs(parsed.query)
        if "url" in query:
            return query["url"][0]

    return absolute_url


def clean_text(value):
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def sanitize_csv_name(csv_file_name):
    cleaned = re.sub(r"[^\w\- ]+", "", csv_file_name).strip().replace(" ", "_")
    return cleaned or "events"


def normalize_city_name(value):
    return clean_text(value).casefold()


def parse_city_filters(value):
    if not value:
        return set()

    return {
        normalized
        for part in re.split(r"[,\n;]+", value)
        for normalized in [normalize_city_name(part)]
        if normalized
    }


def mark_visible_event_cards(page):
    page.wait_for_timeout(1500)
    page.evaluate("""
() => {
  const isRendered = (element) => {
    if (!element) return false;

    let current = element;
    while (current && current.nodeType === 1) {
      const style = window.getComputedStyle(current);
      if (
        current.hidden ||
        current.getAttribute('aria-hidden') === 'true' ||
        style.display === 'none' ||
        style.visibility === 'hidden' ||
        style.opacity === '0'
      ) {
        return false;
      }
      current = current.parentElement;
    }

    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };

  const items = document.querySelectorAll(
    "ul.SearchResultPanelContentEventCardList-module__eventList___2wk-D > li"
  );

  items.forEach((li) => {
    const card = li.querySelector(
      "section[class*='DiscoverHorizontalEventCard-module__cardWrapper']"
    );
    const link = card?.querySelector("a.event-card-link");

    if (card && link && isRendered(li) && isRendered(card) && isRendered(link)) {
      li.setAttribute("data-visible-event-card", "true");
    } else {
      li.remove();
    }
  });
}
""")


def process_event(link):
    try:
        link = normalize_eventbrite_url(link)
        page = StealthyFetcher.fetch(
            link,
            headless=True,
            network_idle=True,
            timeout=60000,
        )

        title_tag = page.css("h1").first
        name = clean_text(title_tag.text) if title_tag else ""
        if not name:
            return None

        event_start_date = ""
        event_end_date = ""
        event_start_time = ""
        event_end_time = ""

        date_block = page.css("div.EventDetails_secondaryText__YIPTc").first
        if date_block:
            schedule_text = date_block.text or " ".join(
                span.text.strip() for span in date_block.css("span") if span.text.strip()
            )
            (
                event_start_date,
                event_start_time,
                event_end_date,
                event_end_time,
            ) = extract_event_schedule(schedule_text)

        if not event_start_date:
            event_start_date = "Multiple dates"

        overview = page.css("div.AboutThisEventEmbedded_container__wdFiD").first
        description = ""
        if overview:
            description = clean_text(" ".join(
                tag.text.strip() for tag in overview.css("p, div") if tag.text.strip()
            ))

        place = ""
        street = ""
        city = ""
        state = ""
        pincode = ""

        address = page.css("address").first
        if address:
            place_tag = address.css("h3").first
            place = clean_text(place_tag.text.replace("#", "")) if place_tag else ""
            p_tags = address.css("p")
            street = clean_text(p_tags[0].text) if len(p_tags) > 0 else ""

            if len(p_tags) > 1:
                address_parts = clean_text(p_tags[1].text).split(",")
                if len(address_parts) >= 1:
                    city = clean_text(address_parts[0].replace("#", ""))
                if len(address_parts) >= 2:
                    tokens = clean_text(address_parts[1]).split()
                    if len(tokens) == 2:
                        state, pincode = tokens
                    elif len(tokens) == 1:
                        if tokens[0].isdigit():
                            pincode = tokens[0]
                        else:
                            state = tokens[0]

        if not place:
            place = "Online Event"

        img_url = ""
        img = page.css("img").first
        if img:
            img_url = clean_image_url(img.attrib.get("src", ""))

        return {
            "Event Name": name,
            "Event Description": description,
            "Event Image": img_url,
            "Event Date": event_start_date,
            "Event Time": event_start_time,
            "Event End Date": event_end_date,
            "Event End Time": event_end_time,
            "Place": place,
            "Street": street,
            "City": city,
            "State": state,
            "Pincode": pincode,
            "Event URL": link,
        }
    except Exception as exc:
        print(f"Error processing {link}: {exc}")
        return None


def fetch_listing_page(page_number):
    return StealthyFetcher.fetch(
        f"{BASE_URL}?page={page_number}",
        headless=True,
        network_idle=True,
        timeout=60000,
        wait_for="ul.SearchResultPanelContentEventCardList-module__eventList___2wk-D",
        page_action=mark_visible_event_cards,
    )


def extract_links_from_page(page):
    ul = page.css("ul.SearchResultPanelContentEventCardList-module__eventList___2wk-D").first
    if not ul:
        return []

    items = ul.css("li[data-visible-event-card='true']")
    links = []

    for li in items:
        horizontal = li.css(
            "section[class*='DiscoverHorizontalEventCard-module__cardWrapper']"
        ).first
        if not horizontal:
            continue

        link_tag = horizontal.css("a.event-card-link").first
        if not link_tag:
            continue

        raw_link = link_tag.attrib.get("href", "").split("?")[0]
        if not raw_link:
            continue

        clean_link = normalize_eventbrite_url(raw_link)
        if not clean_link or canonical_event_url(clean_link) in SKIP_URLS:
            continue

        links.append(clean_link)

    return links


def collect_event_links(
    page_option,
    start_page=None,
    end_page=None,
    event_start_index=None,
    event_end_index=None,
):
    StealthyFetcher.adaptive = True
    all_event_links = []
    seen_event_links = set()

    if page_option == "single_page":
        page = fetch_listing_page(start_page)
        page_links = extract_links_from_page(page)
        event_start_index = event_start_index or 1
        event_end_index = event_end_index or len(page_links)
        return page_links[event_start_index - 1:event_end_index]

    if page_option == "custom":
        page_numbers = range(start_page, end_page + 1)
    else:
        page_numbers = range(1, 1000)

    for page_number in page_numbers:
        page = fetch_listing_page(page_number)
        page_links = extract_links_from_page(page)

        ordered_new_links = []
        for link in page_links:
            if link in seen_event_links:
                continue
            seen_event_links.add(link)
            ordered_new_links.append(link)

        if not ordered_new_links:
            break

        all_event_links.extend(ordered_new_links)
        time.sleep(1)

    return all_event_links


def prepare_event_links(event_links):
    prepared_links = []
    seen_links = set()

    for link in event_links:
        normalized_link = normalize_eventbrite_url(link)
        if not normalized_link:
            continue

        canonical_link = canonical_event_url(normalized_link)
        if not canonical_link or canonical_link in SKIP_URLS or canonical_link in seen_links:
            continue

        seen_links.add(canonical_link)
        prepared_links.append(normalized_link)

    return prepared_links


def write_events_to_csv(output_path, event_links, city_name=None, progress_callback=None):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepared_links = prepare_event_links(event_links)
    total_events = len(prepared_links)
    failure_count = 0
    saved_count = 0
    city_filters = parse_city_filters(city_name)

    if progress_callback:
        progress_callback(0, total_events, None)

    with output_path.open("w", newline="", encoding="utf-8-sig") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        file_handle.flush()

        completed = 0
        worker_count = min(EVENT_DETAIL_WORKERS, total_events) or 1
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            ordered_rows = executor.map(process_event, prepared_links)

            for row in ordered_rows:
                if row:
                    row_city = normalize_city_name(row.get("City"))
                    if not city_filters or row_city in city_filters:
                        writer.writerow(row)
                        file_handle.flush()
                        saved_count += 1
                    else:
                        row = None
                else:
                    failure_count += 1

                completed += 1
                if progress_callback:
                    progress_callback(completed, total_events, row)

    return {
        "filename": output_path.name,
        "file_path": str(output_path),
        "saved_events": saved_count,
        "failed_events": failure_count,
        "total_events": total_events,
        "city_name": city_name or "",
    }


def run_imported_url_scraper(
    csv_file_name,
    output_dir,
    event_urls,
    progress_callback=None,
):
    output_path = Path(output_dir) / f"{sanitize_csv_name(csv_file_name)}.csv"
    return write_events_to_csv(
        output_path=output_path,
        event_links=event_urls,
        city_name=None,
        progress_callback=progress_callback,
    )


def run_scraper(
    csv_file_name,
    page_option,
    output_dir,
    start_page=None,
    end_page=None,
    event_start_index=None,
    event_end_index=None,
    city_name=None,
    progress_callback=None,
):
    output_path = Path(output_dir) / f"{sanitize_csv_name(csv_file_name)}.csv"
    event_links = collect_event_links(
        page_option,
        start_page=start_page,
        end_page=end_page,
        event_start_index=event_start_index,
        event_end_index=event_end_index,
    )
    return write_events_to_csv(
        output_path=output_path,
        event_links=event_links,
        city_name=city_name,
        progress_callback=progress_callback,
    )
