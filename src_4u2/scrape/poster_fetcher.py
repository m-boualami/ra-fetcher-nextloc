import argparse
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from PIL import Image
import io
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


OUTPUT_DIR = Path("posters")
FAILED_LOG = Path("failed.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://ra.co/events/",
}

DELAY       = 1
TIMEOUT     = 15
RETRY_MAX   = 3
IMG_SIZE    = 512
IMG_QUALITY = 85        # JPEG quality

OUTPUT_DIR.mkdir(exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Dowload posters from csv")
    parser.add_argument("--file", required=True, help="path to csv")
    return parser.parse_args()


def compress(raw_bytes: bytes) -> bytes:
    """Resize to IMG_SIZE×IMG_SIZE and re-encode as jpeg"""
    img = Image.open(io.BytesIO(raw_bytes))
    img = img.convert("RGB")                        # drop alpha channel if PNG
    img = img.resize((IMG_SIZE, IMG_SIZE), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=IMG_QUALITY, optimize=True)
    return buf.getvalue()

def download_one(session: requests.Session, image_url: str) -> bool:
    """Download and compress the poster image, return True if successful."""
    filename = Path(image_url.split("/")[-1]).stem + ".jpg"
    shard    = filename[:2]
    dest     = OUTPUT_DIR / shard / filename
    (OUTPUT_DIR / shard).mkdir(exist_ok=True)

    if dest.exists():
        return True

    for attempt in range(RETRY_MAX):
        try:
            r = session.get(image_url, timeout=TIMEOUT)

            if r.status_code == 200:
                try:
                    compressed = compress(r.content)
                except Exception as e:
                    logger.warning(f"Invalid image content : {image_url} — {e}")
                    return False                    # no retry
                dest.write_bytes(compressed)
                return True

            elif r.status_code == 404:
                logger.warning(f"Image not found : {image_url}")
                return False                        # no retry

            else:
                wait = 2 ** attempt
                logger.warning(f"HTTP {r.status_code} - waiting {wait}s (attempt {attempt+1}/{RETRY_MAX})")
                time.sleep(wait)

        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            logger.warning(f"Timeout - waiting {wait}s (attempt {attempt+1}/{RETRY_MAX})")
            time.sleep(wait)

        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            logger.warning(f"Network error : {e} - waiting {wait}s")
            time.sleep(wait)

    return False


def main():
    args = parse_args()

    df   = pd.read_csv(args.file, usecols=["flyer_photo"])
    urls = df["flyer_photo"].dropna().unique().tolist()
    logger.info(f"{len(urls)} images to download (from {args.file})")

    session = requests.Session()
    session.headers.update(HEADERS)

    failed = []

    for url in tqdm(urls, desc="Posters"):
        ok = download_one(session, url)
        if not ok:
            failed.append(url)
        time.sleep(DELAY)

    logger.info(f"Done — {len(urls) - len(failed)}/{len(urls)} successful")

    if failed:
        pd.DataFrame({"url": failed}).to_csv(FAILED_LOG, index=False)
        logger.info(f"{len(failed)} failures → {FAILED_LOG}")


if __name__ == "__main__":
    main()