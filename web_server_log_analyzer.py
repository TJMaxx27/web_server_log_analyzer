import os
import re
import json
import argparse
import logging
from collections import Counter


def parse_log_file(log_file):
    total_requests = 0
    http_methods = Counter()
    ip_addresses = Counter()
    longest_requests = []

    parsed_lines = 0
    failed_lines = 0

    regex_pattern = re.compile(
        r'(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>[A-Z]+) (?P<url>.+?) (HTTP/\d\.\d)" (?P<status>\d{3}) (?P<size>\d+|-) "(?P<referer>-|.*?)" "(?P<user_agent>.*?)" (?P<duration>\d+)?'
    )

    try:
        with open(log_file, 'r') as f:
            for line_number, line in enumerate(f, start=1):
                parts = regex_pattern.match(line)
                if parts:
                    ip = parts.group('ip')
                    timestamp = parts.group('timestamp')
                    method = parts.group('method')
                    url = parts.group('url')
                    status_code = parts.group('status')
                    size = parts.group('size')
                    referer = parts.group('referer')
                    user_agent = parts.group('user_agent')
                    duration = parts.group('duration')

                    if size == '-':
                        size = 0
                    else:
                        size = int(size)

                    if duration is None:
                        duration = 0
                    else:
                        duration = int(duration)

                    total_requests += 1
                    http_methods[method] += 1
                    ip_addresses[ip] += 1
                    longest_requests.append({
                        "ip": ip,
                        "date": timestamp,
                        "method": method,
                        "url": url,
                        "duration": duration
                    })
                    parsed_lines += 1
                else:
                    logging.warning(f"Не удалось распарсить строку {line_number}: {line.strip()}")
                    failed_lines += 1
    except FileNotFoundError:
        logging.error(f"Файл не найден: {log_file}")
        return None
    except Exception as e:
        logging.error(f"Ошибка при чтении файла {log_file}: {e}")
        return None

    if parsed_lines == 0:
        logging.error(f"Файл {log_file} не содержит допустимых строк")
        return None

    top_ips = dict(ip_addresses.most_common(3))
    top_longest = sorted(longest_requests, key=lambda x: x['duration'], reverse=True)[:3]

    return {
        "top_ips": top_ips,
        "top_longest": top_longest,
        "total_stat": dict(http_methods),
        "total_requests": total_requests
    }


def analyze_log_file(log_file):
    stats = parse_log_file(log_file)
    if stats:
        return stats
    else:
        logging.error(f"Не удалось проанализировать файл журнала: {log_file}")
        return {}


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Анализ файлов журнала веб-сервера.')
    parser.add_argument('path', help='Путь к файлу журнала или директории')
    parser.add_argument('--output', help='Путь к директории для сохранения JSON файлов с результатами анализа')
    args = parser.parse_args()

    if os.path.isdir(args.path):
        log_files = [os.path.join(args.path, file) for file in os.listdir(args.path) if
                     os.path.isfile(os.path.join(args.path, file)) and file.endswith('.log')]
    else:
        log_files = [args.path]

    for log_file in log_files:
        stats = analyze_log_file(log_file)
        if stats:
            json_filename = os.path.basename(log_file) + '.json'
            with open(json_filename, 'w') as json_file:
                json.dump(stats, json_file, indent=4)
            print(json.dumps(stats, indent=4))


if __name__ == '__main__':
    main()

