import csv
import io
import json
import re
import threading
import uuid
import zipfile
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET
from django.views.decorators.http import require_POST

from .scraper import normalize_eventbrite_url
from .scraper import process_event
from .scraper import run_imported_url_scraper
from .scraper import run_scraper
from .scraper import sanitize_csv_name

JOB_STATUS_DIR = Path(settings.BASE_DIR) / "job_status"
JOB_STATUS_DIR.mkdir(parents=True, exist_ok=True)
jobs_lock = threading.Lock()


def is_eventbrite_host(host):
    normalized_host = (host or "").lower()
    if normalized_host.startswith("www."):
        normalized_host = normalized_host[4:]

    return normalized_host in {"eventbrite.com", "eventbrite.sg", "eventbrite.ca"}


def home(request):
    return render(request, "home.html")


def get_job_file(job_id):
    return JOB_STATUS_DIR / f"{job_id}.json"


def write_job(job_id, data):
    with jobs_lock:
        get_job_file(job_id).write_text(json.dumps(data), encoding="utf-8")


def read_job(job_id):
    job_file = get_job_file(job_id)
    if not job_file.exists():
        return None

    with jobs_lock:
        return json.loads(job_file.read_text(encoding="utf-8"))


def update_job(job_id, **kwargs):
    with jobs_lock:
        job_file = get_job_file(job_id)
        if not job_file.exists():
            return

        job = json.loads(job_file.read_text(encoding="utf-8"))
        job.update(kwargs)
        job_file.write_text(json.dumps(job), encoding="utf-8")


def update_job_progress(job_id, completed, total, row=None):
    with jobs_lock:
        job_file = get_job_file(job_id)
        if not job_file.exists():
            return

        job = json.loads(job_file.read_text(encoding="utf-8"))
        job.update(
            completed=completed,
            total=total,
            progress=100 if total == 0 else int((completed / total) * 100),
            status="running",
        )

        if row:
            job.setdefault("rows", []).append(row)

        job_file.write_text(json.dumps(job), encoding="utf-8")


def excel_column_index(cell_reference):
    column_letters = "".join(character for character in (cell_reference or "") if character.isalpha())
    if not column_letters:
        return -1

    index = 0
    for character in column_letters.upper():
        index = index * 26 + (ord(character) - ord("A") + 1)

    return index - 1


def parse_xlsx_rows(file_bytes):
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    with zipfile.ZipFile(io.BytesIO(file_bytes)) as workbook:
        shared_strings = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            shared_root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("x:si", namespace):
                shared_strings.append("".join(item.itertext()).strip())

        worksheet_names = sorted(
            name
            for name in workbook.namelist()
            if re.fullmatch(r"xl/worksheets/sheet\d+\.xml", name)
        )
        if not worksheet_names:
            return []

        sheet_root = ElementTree.fromstring(workbook.read(worksheet_names[0]))
        rows = []

        for row in sheet_root.findall(".//x:sheetData/x:row", namespace):
            values = []
            next_index = 0

            for cell in row.findall("x:c", namespace):
                column_index = excel_column_index(cell.attrib.get("r", ""))
                if column_index < 0:
                    column_index = next_index

                while len(values) <= column_index:
                    values.append("")

                cell_type = cell.attrib.get("t")
                if cell_type == "inlineStr":
                    text_value = "".join(cell.itertext()).strip()
                else:
                    value_node = cell.find("x:v", namespace)
                    text_value = value_node.text.strip() if value_node is not None and value_node.text else ""
                    if cell_type == "s" and text_value.isdigit():
                        shared_index = int(text_value)
                        text_value = shared_strings[shared_index] if shared_index < len(shared_strings) else ""

                values[column_index] = text_value
                next_index = column_index + 1

            if any(value.strip() for value in values):
                rows.append(values)

        return rows


def load_tabular_rows(file_name, file_bytes):
    suffix = Path(file_name or "").suffix.lower()
    if suffix == ".csv":
        rows = list(csv.reader(io.StringIO(file_bytes.decode("utf-8-sig"))))
    elif suffix == ".xlsx":
        rows = parse_xlsx_rows(file_bytes)
    else:
        raise ValueError("Only .xlsx and .csv files are supported.")

    if not rows:
        raise ValueError("The uploaded file is empty.")

    header = [str(value or "").strip() for value in rows[0]]
    if not any(header):
        raise ValueError("The uploaded file must have a header row.")

    records = []
    for row in rows[1:]:
        padded_row = list(row) + [""] * max(0, len(header) - len(row))
        record = {
            header[index]: str(padded_row[index] or "").strip()
            for index in range(len(header))
            if header[index]
        }
        if any(value for value in record.values()):
            records.append(record)

    return header, records


def extract_event_urls_from_import(file_name, file_bytes):
    header, records = load_tabular_rows(file_name, file_bytes)
    normalized_header = [re.sub(r"\s+", " ", value).strip().casefold() for value in header]

    url_column_index = -1
    for column_name in ("event url", "event_url", "url"):
        if column_name in normalized_header:
            url_column_index = normalized_header.index(column_name)
            break

    if url_column_index < 0:
        raise ValueError("The file must contain an 'Event URL' column.")

    event_urls = []
    event_url_key = header[url_column_index]
    for row in records:
        raw_url = str(row.get(event_url_key, "") or "").strip()
        if not raw_url:
            continue

        normalized_url = raw_url if "://" in raw_url else f"https://{raw_url}"
        parsed_url = urlparse(normalized_url)
        if not is_eventbrite_host(parsed_url.netloc.lower()):
            continue

        event_urls.append(normalize_eventbrite_url(normalized_url))

    if not event_urls:
        raise ValueError("No valid Eventbrite URLs were found in the uploaded file.")

    return event_urls


def normalize_compare_value(value):
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def build_row_signature(row):
    preferred_keys = [
        "Event URL",
        "Event Name",
        "Event Date",
        "Event Time",
        "Place",
        "Street",
        "City",
        "State",
        "Pincode",
    ]
    values = [normalize_compare_value(row.get(key, "")) for key in preferred_keys if key in row]
    values = [value for value in values if value]
    if values:
        return "|".join(values)

    flattened = [normalize_compare_value(value) for value in row.values() if normalize_compare_value(value)]
    return "|".join(flattened)


def build_similarity_key(row):
    event_url = normalize_compare_value(row.get("Event URL", ""))
    if event_url:
        return f"url:{event_url}"

    event_name = normalize_compare_value(row.get("Event Name", ""))
    event_date = normalize_compare_value(row.get("Event Date", ""))
    city = normalize_compare_value(row.get("City", ""))
    if event_name:
        return f"name:{event_name}|date:{event_date}|city:{city}"

    return build_row_signature(row)


def are_rows_similar(left_row, right_row):
    left_signature = build_row_signature(left_row)
    right_signature = build_row_signature(right_row)
    if not left_signature or not right_signature:
        return False

    if left_signature == right_signature:
        return True

    left_name = normalize_compare_value(left_row.get("Event Name", ""))
    right_name = normalize_compare_value(right_row.get("Event Name", ""))
    if left_name and right_name:
        if left_name == right_name:
            return True
        if SequenceMatcher(None, left_name, right_name).ratio() >= 0.92:
            left_date = normalize_compare_value(left_row.get("Event Date", ""))
            right_date = normalize_compare_value(right_row.get("Event Date", ""))
            return not left_date or not right_date or left_date == right_date

    return SequenceMatcher(None, left_signature, right_signature).ratio() >= 0.96


def compare_uploaded_files(first_name, first_bytes, second_name, second_bytes):
    first_header, first_rows = load_tabular_rows(first_name, first_bytes)
    second_header, second_rows = load_tabular_rows(second_name, second_bytes)

    second_signatures = {}
    second_similarity_groups = {}
    second_name_groups = {}
    for index, row in enumerate(second_rows):
        second_signatures.setdefault(build_row_signature(row), []).append(index)
        second_similarity_groups.setdefault(build_similarity_key(row), []).append(index)
        event_name = normalize_compare_value(row.get("Event Name", ""))
        if event_name:
            second_name_groups.setdefault(event_name, []).append(index)

    duplicate_first = set()
    duplicate_second = set()
    similar_first = set()
    similar_second = set()

    for first_index, first_row in enumerate(first_rows):
        signature = build_row_signature(first_row)
        if signature and signature in second_signatures:
            duplicate_first.add(first_index)
            duplicate_second.update(second_signatures[signature])
            continue

        candidate_indexes = list(second_similarity_groups.get(build_similarity_key(first_row), []))
        first_event_name = normalize_compare_value(first_row.get("Event Name", ""))
        if first_event_name:
            for second_index in second_name_groups.get(first_event_name, []):
                if second_index not in candidate_indexes:
                    candidate_indexes.append(second_index)

        for second_index in candidate_indexes:
            if are_rows_similar(first_row, second_rows[second_index]):
                similar_first.add(first_index)
                similar_second.add(second_index)
                break

    return {
        "first_file": {
            "name": first_name,
            "headers": first_header,
            "rows": first_rows,
            "duplicate_indexes": sorted(duplicate_first),
            "similar_indexes": sorted(similar_first),
        },
        "second_file": {
            "name": second_name,
            "headers": second_header,
            "rows": second_rows,
            "duplicate_indexes": sorted(duplicate_second),
            "similar_indexes": sorted(similar_second),
        },
        "summary": {
            "first_total": len(first_rows),
            "second_total": len(second_rows),
            "duplicate_count": len(duplicate_first),
            "similar_count": len(similar_first),
        },
    }


def run_scraper_job(
    job_id,
    csv_file_name,
    page_option,
    output_dir,
    start_value,
    end_value,
    event_start_value,
    event_end_value,
    city_name,
):
    try:
        result = run_scraper(
            csv_file_name=csv_file_name,
            page_option=page_option,
            output_dir=output_dir,
            start_page=start_value,
            end_page=end_value,
            event_start_index=event_start_value,
            event_end_index=event_end_value,
            city_name=city_name,
            progress_callback=lambda completed, total, row=None: update_job_progress(
                job_id, completed, total, row
            ),
        )
    except Exception as exc:
        update_job(job_id, status="error", message=f"Scraping failed: {exc}")
        return

    if result["saved_events"] == 0:
        output_file = output_dir / result["filename"]
        if output_file.exists():
            output_file.unlink()

        city_message = (
            f" for city filter {result['city_name']}"
            if result["city_name"]
            else ""
        )
        update_job(
            job_id,
            status="error",
            progress=100,
            message=f"No events found{city_message}. CSV was not created.",
            result=result,
        )
        return

    update_job(
        job_id,
        status="completed",
        progress=100,
        message=(
            f"Successfully downloaded {result['filename']}"
            if not result["city_name"]
            else f"Successfully downloaded {result['filename']} for city {result['city_name']}"
        ),
        result=result,
    )


def run_import_job(job_id, csv_file_name, output_dir, imported_event_urls):
    try:
        result = run_imported_url_scraper(
            csv_file_name=csv_file_name,
            output_dir=output_dir,
            event_urls=imported_event_urls,
            progress_callback=lambda completed, total, row=None: update_job_progress(
                job_id, completed, total, row
            ),
        )
    except Exception as exc:
        update_job(job_id, status="error", message=f"Import failed: {exc}")
        return

    if result["saved_events"] == 0:
        output_file = output_dir / result["filename"]
        if output_file.exists():
            output_file.unlink()

        update_job(
            job_id,
            status="error",
            progress=100,
            message="No valid events were imported. CSV was not created.",
            result=result,
        )
        return

    update_job(
        job_id,
        status="completed",
        progress=100,
        message=f"Successfully generated {result['filename']} from the imported file.",
        result=result,
    )


@require_POST
def download_csv(request):
    csv_file_name = request.POST.get("csv_file_name", "").strip()
    page_option = request.POST.get("page_option", "all")
    city_name = request.POST.get("city_name", "").strip()
    start_page = request.POST.get("start_page", "").strip()
    end_page = request.POST.get("end_page", "").strip()
    single_page = request.POST.get("single_page", "").strip()
    event_start = request.POST.get("event_start", "").strip()
    event_end = request.POST.get("event_end", "").strip()
    import_file = request.FILES.get("event_file")

    if not csv_file_name:
        return JsonResponse(
            {"status": "error", "message": "CSV file name is required."},
            status=400,
        )

    start_value = None
    end_value = None
    event_start_value = None
    event_end_value = None
    imported_event_urls = None

    if page_option == "custom":
        if not start_page or not end_page:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "Starting page and ending page are required for custom pages.",
                },
                status=400,
            )

        try:
            start_value = int(start_page)
            end_value = int(end_page)
        except ValueError:
            return JsonResponse(
                {"status": "error", "message": "Page values must be numbers."},
                status=400,
            )

        if start_value <= 0 or end_value <= 0 or start_value > end_value:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "Enter a valid page range. Start must be less than or equal to end.",
                },
                status=400,
            )

    if page_option == "single_page":
        if not single_page:
            return JsonResponse(
                {"status": "error", "message": "Single page number is required."},
                status=400,
            )

        try:
            start_value = int(single_page)
            event_start_value = int(event_start) if event_start else 1
            event_end_value = int(event_end) if event_end else None
        except ValueError:
            return JsonResponse(
                {"status": "error", "message": "Single page and event range values must be numbers."},
                status=400,
            )

        if start_value <= 0 or event_start_value <= 0 or (event_end_value is not None and event_end_value <= 0):
            return JsonResponse(
                {"status": "error", "message": "Enter positive numbers for page and event range."},
                status=400,
            )

        if event_end_value is not None and event_start_value > event_end_value:
            return JsonResponse(
                {"status": "error", "message": "Event start range must be less than or equal to event end range."},
                status=400,
            )

    if page_option == "import_file":
        if not import_file:
            return JsonResponse(
                {"status": "error", "message": "Excel file is required for import mode."},
                status=400,
            )

        try:
            imported_event_urls = extract_event_urls_from_import(
                import_file.name,
                import_file.read(),
            )
        except ValueError as exc:
            return JsonResponse(
                {"status": "error", "message": str(exc)},
                status=400,
            )

    output_dir = Path(settings.MEDIA_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{sanitize_csv_name(csv_file_name)}.csv"

    if output_path.exists():
        return JsonResponse(
            {"status": "error", "message": "File name already exists."},
            status=400,
        )

    job_id = uuid.uuid4().hex
    write_job(
        job_id,
        {
            "status": "queued",
            "progress": 0,
            "completed": 0,
            "total": 0,
            "message": "Scraping started.",
            "result": None,
            "rows": [],
            "page_option": page_option,
            "city_name": city_name,
            "start_page": start_value,
            "end_page": end_value,
            "event_start": event_start_value,
            "event_end": event_end_value,
            "imported_total": len(imported_event_urls or []),
        },
    )

    if page_option == "import_file":
        worker = threading.Thread(
            target=run_import_job,
            args=(
                job_id,
                csv_file_name,
                output_dir,
                imported_event_urls,
            ),
            daemon=True,
        )
    else:
        worker = threading.Thread(
            target=run_scraper_job,
            args=(
                job_id,
                csv_file_name,
                page_option,
                output_dir,
                start_value,
                end_value,
                event_start_value,
                event_end_value,
                city_name,
            ),
            daemon=True,
        )
    worker.start()

    return JsonResponse(
        {
            "status": "accepted",
            "message": "Scraping started.",
            "data": {
                "job_id": job_id,
                "csv_file_name": csv_file_name,
                "page_option": page_option,
                "city_name": city_name,
                "start_page": start_value,
                "end_page": end_value,
                "event_start": event_start_value,
                "event_end": event_end_value,
                "imported_total": len(imported_event_urls or []),
                "rows": [],
            },
        }
    )


@require_POST
def single_event_data(request):
    event_urls = [
        value.strip()
        for value in request.POST.getlist("single_event_url")
        if value.strip()
    ]

    if not event_urls:
        return JsonResponse(
            {"status": "error", "message": "Single event URL is required."},
            status=400,
        )

    rows = []
    failed_urls = []

    for event_url in event_urls:
        normalized_url = event_url if "://" in event_url else f"https://{event_url}"
        parsed_url = urlparse(normalized_url)
        host = parsed_url.netloc.lower()

        if not is_eventbrite_host(host):
            failed_urls.append(event_url)
            continue

        normalized_url = normalize_eventbrite_url(normalized_url)
        event_data = process_event(normalized_url)
        if not event_data:
            failed_urls.append(normalized_url)
            continue

        rows.append(event_data)

    if not rows:
        return JsonResponse(
            {"status": "error", "message": "Unable to load data for the provided event URL(s)."},
            status=400,
        )

    if failed_urls:
        message = f"Loaded {len(rows)} event(s). Skipped {len(failed_urls)} invalid or failed URL(s)."
    else:
        message = f"Loaded {len(rows)} event(s)."

    return JsonResponse(
        {
            "status": "success",
            "message": message,
            "data": {
                "record": rows[0],
                "rows": rows,
                "failed_urls": failed_urls,
            },
        }
    )


@require_POST
def compare_files(request):
    first_file = request.FILES.get("compare_file_one")
    second_file = request.FILES.get("compare_file_two")

    if not first_file or not second_file:
        return JsonResponse(
            {"status": "error", "message": "Both files are required for comparison."},
            status=400,
        )

    try:
        result = compare_uploaded_files(
            first_file.name,
            first_file.read(),
            second_file.name,
            second_file.read(),
        )
    except ValueError as exc:
        return JsonResponse(
            {"status": "error", "message": str(exc)},
            status=400,
        )

    return JsonResponse(
        {
            "status": "success",
            "message": (
                f"Compared {result['summary']['first_total']} row(s) with "
                f"{result['summary']['second_total']} row(s). Found "
                f"{result['summary']['duplicate_count']} duplicate and "
                f"{result['summary']['similar_count']} similar row(s)."
            ),
            "data": result,
        }
    )


@require_GET
def download_progress(request, job_id):
    job = read_job(job_id)

    if not job:
        return JsonResponse(
            {"status": "error", "message": "Job not found."},
            status=404,
        )

    response = {
        "status": job["status"],
        "message": job["message"],
        "progress": job["progress"],
        "completed": job["completed"],
        "total": job["total"],
        "data": {
            "page_option": job["page_option"],
            "city_name": job.get("city_name", ""),
            "start_page": job["start_page"],
            "end_page": job["end_page"],
            "event_start": job.get("event_start"),
            "event_end": job.get("event_end"),
            "imported_total": job.get("imported_total", 0),
            "rows": job.get("rows", []),
        },
    }

    if job["result"]:
        response["data"].update(job["result"])
        media_url = settings.MEDIA_URL if settings.MEDIA_URL.endswith("/") else f"{settings.MEDIA_URL}/"
        response["data"]["download_url"] = f"{media_url}{job['result']['filename']}"

    return JsonResponse(response)
