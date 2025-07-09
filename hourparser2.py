import re
import os
import glob
import tarfile
from datetime import datetime
from collections import defaultdict

class PostgresLogParser:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.log_pattern = re.compile(
            r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: UTC)?).*?(?:statement|execute \S+):\s+(.*?)(?:;|$)',
            re.IGNORECASE
        )
        self.detail_pattern = re.compile(
            r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: UTC)?).*?DETAIL:\s+Parameters:\s+(.*)$',
            re.IGNORECASE
        )
        self.string_pattern = re.compile(r"'(?:''|[^'])*'")
        self.number_pattern = re.compile(r'\b\d+\b')
        self.float_pattern = re.compile(r'\b\d+\.\d+\b')
        
        self.dates_seen = set()
        self.sql_cache = {}
        self.file_buffers = defaultdict(list)
        self.detail_buffers = defaultdict(list)
        self.detail_counts = defaultdict(lambda: defaultdict(int))
        self.buffer_size = 1000
        
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.base_name = os.path.splitext(os.path.basename(log_file_path))[0]
        self.archive_name = f"{self.base_name}-hour_parser-{self.run_timestamp}.tar.gz"
        self.summary_filename = f"summary_hour_{self.base_name}.log"
        self.detail_summary_filename = f"detail_summary_{self.base_name}.log"

    def parse(self):
        output_files = {}
        current_query = None
        current_timestamp = None

        try:
            with open(self.log_file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    detail_match = self.detail_pattern.match(line)
                    if detail_match:
                        timestamp, params = detail_match.groups()
                        self.process_detail(timestamp.strip(), params.strip())
                        continue

                    # Пропуск prepare-запросов
                    if "prepare:" in line.lower():
                        continue

                    if line[0].isdigit():
                        if current_query:
                            self.buffer_query(current_query, output_files)
                        match = self.log_pattern.match(line)
                        if match:
                            timestamp, sql = match.groups()
                            current_timestamp = timestamp.strip()
                            current_query = {'timestamp': current_timestamp, 'sql': sql.strip()}
                        else:
                            current_query = None
                    elif current_query:
                        current_query['sql'] += ' ' + line
        finally:
            if current_query:
                self.buffer_query(current_query, output_files)
            self.flush_buffers(output_files)
            self.flush_detail_buffers()
            for file in output_files.values():
                file.close()

            self.generate_combined_summary()
            self.generate_detail_summary()
            self.compress_results()
            self.cleanup_files()

    def process_detail(self, timestamp, params):
        if any(op in params.upper() for op in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']):
            date_part = timestamp.split()[0]
            self.dates_seen.add(date_part)
            
            normalized = self.normalize_sql(params)
            self.detail_counts[date_part][normalized] += 1
            
            detail_filename = f"DETAIL_{date_part}.log"
            self.detail_buffers[detail_filename].append(f"{timestamp} | {params}\n")
            
            if len(self.detail_buffers[detail_filename]) >= self.buffer_size:
                self.flush_detail_buffer(detail_filename)

    def flush_detail_buffer(self, filename=None):
        if filename:
            buffer = self.detail_buffers.get(filename, [])
            if buffer:
                with open(filename, 'a', encoding='utf-8') as f:
                    f.writelines(buffer)
                self.detail_buffers[filename] = []
        else:
            for filename, buffer in self.detail_buffers.items():
                if buffer:
                    with open(filename, 'a', encoding='utf-8') as f:
                        f.writelines(buffer)
                    self.detail_buffers[filename] = []

    def flush_detail_buffers(self):
        self.flush_detail_buffer()

    def buffer_query(self, query, output_files):
        if not query or 'sql' not in query:
            return

        sql = query['sql'].rstrip(';')
        first_word = sql.split()[0].upper() if sql else ''
        
        # Пропуск PREPARE-запросов
        if first_word == 'PREPARE':
            return
        
        if first_word == 'WITH':
            operator = 'SELECT'
        elif first_word in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
            operator = first_word
        else:
            operator = 'OTHER'

        date_part = query['timestamp'].split()[0]
        self.dates_seen.add(date_part)

        filename = f"{operator}_{date_part}.log"
        if filename not in output_files:
            output_files[filename] = open(filename, 'a', encoding='utf-8')

        self.file_buffers[filename].append(f"{query['timestamp']} | {sql}\n")
        if len(self.file_buffers[filename]) >= self.buffer_size:
            output_files[filename].writelines(self.file_buffers[filename])
            self.file_buffers[filename] = []

    def flush_buffers(self, output_files):
        for filename, buffer in self.file_buffers.items():
            if buffer and filename in output_files:
                output_files[filename].writelines(buffer)
        self.file_buffers.clear()

    def normalize_sql(self, sql):
        if sql in self.sql_cache:
            return self.sql_cache[sql]
        
        normalized = self.string_pattern.sub('$str', sql)
        normalized = self.float_pattern.sub('$num', normalized)
        normalized = self.number_pattern.sub('$num', normalized)
        normalized = ' '.join(normalized.split())
        
        self.sql_cache[sql] = normalized
        return normalized

    def generate_combined_summary(self):
        stats = defaultdict(lambda: defaultdict(int))
        cache = self.sql_cache

        for date in self.dates_seen:
            for file_path in glob.glob(f'*_{date}.log'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.split(' | ', 1)
                        if len(parts) < 2:
                            continue

                        timestamp, sql = parts
                        sql = sql.strip()
                        
                        if sql in cache:
                            normalized_sql = cache[sql]
                        else:
                            normalized_sql = self.normalize_sql(sql)
                        
                        try:
                            ts_clean = timestamp[:19]
                            dt = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
                            hour_key = dt.replace(minute=0, second=0).strftime('%Y-%m-%d %H:%M')
                        except ValueError:
                            continue

                        stats[hour_key][normalized_sql] += 1

        with open(self.summary_filename, 'w', encoding='utf-8') as sf:
            for hour in sorted(stats):
                for sql, count in sorted(stats[hour].items()):
                    sf.write(f"{hour} | {sql} | выполнился {count} раз\n")

    def generate_detail_summary(self):
        with open(self.detail_summary_filename, 'w', encoding='utf-8') as sf:
            for date in sorted(self.detail_counts):
                for params, count in sorted(self.detail_counts[date].items()):
                    sf.write(f"{date} | {params} | встретился {count} раз\n")

    def compress_results(self):
        files_to_archive = []
        if os.path.exists(self.summary_filename):
            files_to_archive.append(self.summary_filename)
        if os.path.exists(self.detail_summary_filename):
            files_to_archive.append(self.detail_summary_filename)
        
        for date in self.dates_seen:
            for operator in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'OTHER']:
                filename = f"{operator}_{date}.log"
                if os.path.exists(filename):
                    files_to_archive.append(filename)
            
            detail_filename = f"DETAIL_{date}.log"
            if os.path.exists(detail_filename):
                files_to_archive.append(detail_filename)
        
        if files_to_archive:
            with tarfile.open(self.archive_name, "w:gz") as tar:
                for file in files_to_archive:
                    tar.add(file)
            print(f"Результаты сохранены в архив: {self.archive_name}")

    def cleanup_files(self):
        for file in glob.glob("*.log"):
            if file != self.log_file_path and file != self.archive_name:
                try:
                    os.remove(file)
                except Exception as e:
                    print(f"Ошибка при удалении файла {file}: {e}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Использование: python hour_parser.py <путь_к_файлу_лога>")
        sys.exit(1)

    parser = PostgresLogParser(sys.argv[1])
    parser.parse()
