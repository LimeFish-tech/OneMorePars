import sys
import re
from collections import defaultdict

def determine_file_type(timestamp_str):
    """Определяет тип файла по формату временной метки"""
    if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", timestamp_str):
        return "minute"
    elif re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", timestamp_str):
        return "hour"
    elif re.match(r"\d{4}-\d{2}-\d{2}", timestamp_str):
        return "day"
    return "unknown"

def extract_time_key(timestamp_str, file_type):
    """Извлекает ключ времени в зависимости от типа файла"""
    if file_type == "minute":
        return timestamp_str[11:16]  # HH:MM
    elif file_type == "hour":
        return timestamp_str[11:13] + ":00"  # HH:00
    elif file_type == "day":
        return "daily"
    return ""

def process_files(file_paths):
    """Обрабатывает файлы и возвращает объединенные данные"""
    # Словарь для хранения результатов: (query, time_key) -> {file_index: (timestamp, count)}
    combined_data = defaultdict(lambda: [("", 0)] * len(file_paths))
    file_types = []
    
    # Определяем типы файлов
    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if not first_line:
                file_types.append("unknown")
                continue
                
            parts = first_line.split(' | ', 2)
            if len(parts) < 3:
                file_types.append("unknown")
            else:
                file_types.append(determine_file_type(parts[0]))
    
    # Обрабатываем каждый файл
    for file_index, file_path in enumerate(file_paths):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split(' | ', 2)
                if len(parts) < 3:
                    continue
                    
                timestamp_str = parts[0]
                sql = parts[1]
                
                # Извлекаем количество выполнений
                count_part = parts[2]
                try:
                    count = int(count_part.split()[-2])  # Предпоследнее слово в последней части
                except (ValueError, IndexError):
                    continue
                
                # Получаем тип файла и ключ времени
                file_type = file_types[file_index]
                time_key = extract_time_key(timestamp_str, file_type)
                
                # Обновляем данные
                key = (sql, time_key)
                current_data = list(combined_data[key])
                current_data[file_index] = (timestamp_str, count)
                combined_data[key] = current_data
                
    return combined_data, file_paths

def save_combined_results(data, file_paths, output_file="combined_results.log"):
    """Сохраняет объединенные результаты в файл"""
    with open(output_file, 'w', encoding='utf-8') as f:
        # Заголовок с именами файлов
        header = " | ".join([f"TimeStamp_{i+1}" for i in range(len(file_paths))])
        header += " | SQL_Query | " + " | ".join([f"Count_{i+1}" for i in range(len(file_paths))])
        f.write(header + "\n")
        
        # Данные
        for (sql, time_key), file_data in data.items():
            timestamps = []
            counts = []
            
            for timestamp, count in file_data:
                timestamps.append(timestamp)
                counts.append(str(count))
            
            timestamp_line = " | ".join(timestamps)
            count_line = " | ".join(counts)
            f.write(f"{timestamp_line} | {sql} | {count_line}\n")

def main():
    if len(sys.argv) < 2 or len(sys.argv) > 8:
        print("Использование: python combiner.py file1 [file2 ... file7]")
        print("Максимальное количество файлов: 7")
        return

    file_paths = sys.argv[1:]
    combined_data, processed_files = process_files(file_paths)
    save_combined_results(combined_data, processed_files)
    print(f"Результаты объединены в файл: combined_results.log")

if __name__ == "__main__":
    main()
