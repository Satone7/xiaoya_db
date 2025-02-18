import argparse
import os
import sys
import glob
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import logging
from queue import Queue, Empty as QueueEmpty
from threading import Thread, Event
import threading
import time
import multiprocessing

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,  # 明确指定输出到标准输出
)
logger = logging.getLogger("clean_strm")
# logging.getLogger("chardet.charsetprober").disabled = True

def check_url_exists(url):
    """Check if the URL resource exists"""
    try:
        # Use HEAD request instead of GET to avoid downloading the file
        response = requests.head(url, timeout=10)

        if response.status_code != 500:
            # Check if response is JSON (usually indicates an error)
            content_type = response.headers.get('content-type', '')
            if 'application/json' in content_type:
                return False
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking URL: {url}, error: {str(e)}")
        return False

def process_strm_file(strm_path):
    """Process a single .strm file"""
    worker_id = threading.get_ident() % 1000  # 获取简化的线程ID
    try:
        # logger.info(f"[Worker {worker_id}] Processing file: {strm_path}")
        # Read URL from .strm file
        with open(strm_path, 'r', encoding='utf-8') as f:
            url = f.read().strip()

        # If resource doesn't exist
        if not check_url_exists(url):
            # Get filename without extension
            base_name = os.path.splitext(strm_path)[0]
            # Get all related files
            related_files = glob.glob(f"{base_name}.*")

            # Delete all related files
            for file_path in related_files:
                try:
                    os.remove(file_path)
                    logger.info(f"[Worker {worker_id}] Deleted file: {file_path}")
                except Exception as e:
                    logger.error(f"[Worker {worker_id}] Failed to delete file: {file_path}, error: {str(e)}")
            return True
        return False
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Failed to process file: {strm_path}, error: {str(e)}")
        return False

def collect_strm_files(target_dir, file_queue, done_event, total_counter):
    """Collect .strm files and put them into queue"""
    try:
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith('.strm'):
                    file_path = os.path.join(root, file)
                    file_queue.put(file_path)
                    with total_counter.get_lock():
                        total_counter.value += 1
    except Exception as e:
        logger.error(f"Error collecting files: {str(e)}")
    finally:
        done_event.set()

def process_files_from_queue(file_queue, done_event, executor, total_counter):
    """Process files from queue using thread pool"""
    processed_count = 0
    cleaned_count = 0
    progress_lock = threading.Lock()

    def print_progress():
        with progress_lock:
            current_total = total_counter.value
            progress = (processed_count / current_total) * 100 if current_total > 0 else 0
            bar_length = 40
            filled_length = int(bar_length * progress // 100)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            # 将进度条输出到标准错误
            sys.stderr.write(f"\rProgress: |{bar}| {progress:.1f}% ({processed_count}/{current_total}) Cleaned: {cleaned_count}")
            sys.stderr.flush()

    def progress_updater():
        while not done_event.is_set() or not file_queue.empty():
            print_progress()
            time.sleep(0.5)
        print_progress()

    progress_thread = threading.Thread(target=progress_updater, daemon=True)
    progress_thread.start()

    def process_result(future):
        nonlocal processed_count, cleaned_count
        try:
            result = future.result()
            with progress_lock:
                processed_count += 1
                if result:
                    cleaned_count += 1
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")

    # 提交任务并添加回调
    while True:
        try:
            if done_event.is_set() and file_queue.empty():
                break
            file_path = file_queue.get(timeout=1)
            future = executor.submit(process_strm_file, file_path)
            future.add_done_callback(process_result)
        except QueueEmpty:
            continue

    progress_thread.join()
    print()
    return cleaned_count, processed_count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--media",
        metavar="<folder>",
        type=str,
        default=None,
        required=True,
        help="Path to store downloaded media files [Default: %(default)s]",
    )
    parser.add_argument(
        "--count",
        metavar="[number]",
        type=int,
        default=100,
        help="Max concurrent HTTP Requests [Default: %(default)s]",
    )
    parser.add_argument(
        "--debug",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Verbose debug [Default: %(default)s]",
    )
    parser.add_argument(
        "--db",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="<Python3.12+ required> Save into DB [Default: %(default)s]",
    )
    parser.add_argument(
        "--nfo",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Download NFO [Default: %(default)s]",
    )
    parser.add_argument(
        "--url",
        metavar="[url]",
        type=str,
        default=None,
        help="Download path [Default: %(default)s]",
    )
    parser.add_argument(
        "--purge",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=True,
        help="Purge removed files [Default: %(default)s]",
    )
    parser.add_argument(
        "--all",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Download all folders [Default: %(default)s]",
    )
    parser.add_argument(
        "--location",
        metavar="<folder>",
        type=str,
        default=None,
        required=None,
        help="Path to store database files [Default: %(default)s]",
    )
    parser.add_argument(
        "--paths",
        metavar="<file>",
        type=str,
        help="Bitmap of paths or a file containing paths to be selected (See paths.example)",
    )

    args = parser.parse_args()
    if args.debug:
        logging.getLogger("clean_strm").setLevel(logging.DEBUG)

    target_dir = args.media
    file_queue = Queue()
    done_event = Event()

    # 使用共享变量跟踪总文件数
    total_counter = multiprocessing.Value('i', 0)

    collector_thread = Thread(
        target=collect_strm_files,
        args=(target_dir, file_queue, done_event, total_counter)
    )
    collector_thread.start()

    with ThreadPoolExecutor(max_workers=4) as executor:
        cleaned_count, processed_count = process_files_from_queue(
            file_queue, done_event, executor, total_counter
        )

    collector_thread.join()
    logger.info(f"Final total files: {total_counter.value}")
    logger.info(f"Cleanup completed! Processed {processed_count} files, cleaned {cleaned_count} invalid resources")

if __name__ == "__main__":
    main()