import re
from collections import defaultdict
import os

def extract_time_key(timestamp_str):
    """Извлекает ключ времени из временной метки (часы:минуты)"""
    if not timestamp_str:
        return ""
    
    # Пытаемся найти время в формате ЧЧ:ММ
    time_match = re.search(r"(\d{2}:\d{2})", timestamp_str)
    if time_match:
        return time_match.group(1)
    
    # Для дневных меток возвращаем "daily"
    if re.match(r"\d{4}-\d{2}-\d{2}", timestamp_str):
        return "daily"
    
    return ""

def group_combined_file(input_file, output_file):
    """Группирует строки по SQL-запросам с сортировкой по времени"""
    # Словарь для хранения данных: sql -> [сортированные строки]
    grouped_data = defaultdict(list)
    
    # Определение количества файлов из заголовка
    with open(input_file, 'r', encoding='utf-8') as f:
        header = f.readline().strip()
        columns = header.split(' | ')
        
        # Определение количества временных меток
        time_cols = [col for col in columns if col.startswith('TimeStamp')]
        n_files = len(time_cols)
        
        # Индекс начала SQL-запроса
        sql_index = n_files
        count_index = sql_index + 1
        
        # Обработка строк
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split(' | ')
            if len(parts) < n_files * 2 + 1:
                continue
                
            # Извлекаем временные метки, запрос и счетчики
            timestamps = parts[:n_files]
            sql = parts[sql_index]
            counts = parts[count_index:count_index + n_files]
            
            # Создаем ключ для сортировки (первая непустая временная метка)
            sort_key = ""
            for ts in timestamps:
                if ts:
                    sort_key = extract_time_key(ts)
                    break
            
            # Сохраняем данные для группировки
            grouped_data[sql].append((sort_key, timestamps, counts))

    # Сортировка и запись результатов
    with open(output_file, 'w', encoding='utf-8') as f_out:
        # Записываем заголовок
        f_out.write(header + '\n')
        
        # Сортируем группы по SQL-запросу
        for sql in sorted(grouped_data.keys()):
            # Сортируем строки внутри группы по временному ключу
            group_rows = grouped_data[sql]
            group_rows.sort(key=lambda x: x[0] or "00:00")
            
            # Записываем отсортированные строки
            for _, timestamps, counts in group_rows:
                line = " | ".join(timestamps) + " | " + sql + " | " + " | ".join(counts)
                f_out.write(line + '\n')

if __name__ == "__main__":
    input_file = "combined_results.log"
    output_file = "grouped_combined_results.log"
    
    if not os.path.exists(input_file):
        print(f"Ошибка: Файл {input_file} не найден!")
        exit(1)
    
    group_combined_file(input_file, output_file)
    print(f"Результат группировки сохранен в: {output_file}")
