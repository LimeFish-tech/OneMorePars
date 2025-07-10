import sys
import re
from collections import defaultdict
from datetime import datetime

def parse_summary_file(file_path):
    """Парсинг summary-файла и извлечение данных"""
    data = defaultdict(dict)
    pattern = re.compile(r'^(.*?) \| (.*?) \| выполнился (\d+) раз$')
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            match = pattern.match(line.strip())
            if match:
                timestamp = match.group(1).strip()
                query = match.group(2).strip()
                count = int(match.group(3))
                
                # Нормализация временной метки для корректной сортировки
                if ':' in timestamp:  # Минутные/часовые данные
                    try:
                        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M')
                        normalized_timestamp = dt.strftime('%Y-%m-%d %H:%M')
                    except ValueError:
                        normalized_timestamp = timestamp
                else:  # Дневные данные
                    normalized_timestamp = timestamp
                
                data[normalized_timestamp][query] = count
    
    return data

def compare_summary_files(file_paths):
    """Сравнение данных из нескольких summary-файлов"""
    # Сбор данных из всех файлов
    all_data = []
    for path in file_paths:
        all_data.append(parse_summary_file(path))
    
    # Сбор всех уникальных временных меток и запросов
    all_timestamps = set()
    all_queries = set()
    
    for data in all_data:
        for timestamp, queries in data.items():
            all_timestamps.add(timestamp)
            all_queries.update(queries.keys())
    
    # Сортировка временных меток
    sorted_timestamps = sorted(all_timestamps, key=lambda ts: (
        datetime.strptime(ts, '%Y-%m-%d %H:%M') if ':' in ts 
        else datetime.strptime(ts, '%Y-%m-%d')
    ))
    
    # Создание матрицы сравнения
    comparison_matrix = []
    
    for timestamp in sorted_timestamps:
        for query in sorted(all_queries):
            counts = []
            for data in all_data:
                # Получение количества выполнений для данного timestamp и query
                count = data.get(timestamp, {}).get(query, 0)
                counts.append(str(count))
            
            # Добавляем только строки с ненулевыми значениями
            if any(c != '0' for c in counts):
                comparison_matrix.append(
                    f"{timestamp} | {query} | {' | '.join(counts)}"
                )
    
    return comparison_matrix

def main():
    # Проверка аргументов командной строки
    if len(sys.argv) < 2 or len(sys.argv) > 8:
        print("Использование: python summary_comparer.py file1 [file2 ... file7]")
        print("Максимальное количество файлов: 7")
        sys.exit(1)
    
    file_paths = sys.argv[1:]
    
    # Сравнение данных
    results = compare_summary_files(file_paths)
    
    # Вывод результатов
    for line in results:
        print(line)
    
    print(f"\nОбработано файлов: {len(file_paths)}")
    print(f"Всего строк сравнения: {len(results)}")

if __name__ == "__main__":
    main()
