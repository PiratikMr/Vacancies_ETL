import subprocess
import datetime
import re
import argparse

def get_hdfs_folders(hdfs_path):
    """Получает список папок из HDFS."""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', hdfs_path],
            capture_output=True, text=True, check=True
        )
        output = result.stdout
        folder_pattern = re.compile(r'/hhapi/([^/]+)$', re.MULTILINE)
        
        folders = folder_pattern.findall(output)
        return folders
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении hdfs dfs -ls: {e}")
        return []

def delete_hdfs_folders(hdfs_path, date_limit, remove_folders=False):
    folders = get_hdfs_folders(hdfs_path)
    if not folders:
        print("Нет папок для обработки.")
        return
    
    print(f"Найдены папки: {folders}")
    
    date_limit = datetime.datetime.strptime(date_limit, '%Y-%m-%d').date()

    for folder_date_str in folders:
        try:
            folder_date = datetime.datetime.strptime(folder_date_str, '%Y-%m-%d').date()
            
            if folder_date < date_limit:
                folder_to_remove = f'{hdfs_path}/{folder_date_str}'
                print(f"Папка {folder_to_remove} будет удалена")
                
                if remove_folders:
                    delete_command = ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-rm', '-r', folder_to_remove]
                    subprocess.run(delete_command, check=True, capture_output=True)
                    print(f"Удалена папка: {folder_to_remove}")
        except ValueError:
                print(f"Неверный формат даты {folder_date_str}. Пропускаем.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Удаляет устаревшие папки из HDFS.")
    parser.add_argument("--hdfs_path", required=True, help="Путь в HDFS, где лежат папки (например: /hhapi/)")
    parser.add_argument("--date_limit", required=True, help="Дата (YYYY-MM-DD), до которой папки будут удаляться")
    parser.add_argument("--remove", action="store_true", help="Удалить папки. Если этот флаг не указан, то будут только напечатаны папки которые подлежат удалению")

    args = parser.parse_args()
    
    delete_hdfs_folders(args.hdfs_path, args.date_limit, args.remove)