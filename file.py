import argparse
import random
import string
import os
import time
import pandas as pd
from datetime import date
from sqlalchemy import create_engine


class EYFile:

    def __init__(self, number_files, number_lines):
        self.number_files = number_files
        self.number_lines = number_lines

    def create_files(self):
        os.makedirs('files', exist_ok=True)  # Создание директории для генерируемых файлов.
        for i in range(self.number_files):
            # Генерируем название файла
            with open(os.path.join('files', f'{i}.txt'), 'w') as f:
                for _ in range(self.number_lines):
                    # Генерируем контент, приводим к необходимой структуре и записываем в файл.
                    content = self._create_content()
                    str_file = '||'.join(content)
                    f.write(str_file + '\n')

    def _random_date(self):
        # Генерируем случайную дату за последние 5 лет
        today = date.today()
        start_date = today.replace(year=today.year - 5).toordinal()
        end_date = date.today().toordinal()
        # через функцию toordinal() высчитывается промежуток целых чисел,
        # в котором и генерируется случайная дата
        res = date.fromordinal(random.randint(start_date, end_date))
        return res

    def _create_content(self):
        # Создаем пустой список, куда добавляем генерируемый контент
        arr = []
        # номер первой буквы алфавита в русском языке в таблице Unicode для последующей генерации русского алфавита
        a = ord('А')
        # Переводим дату в строку
        arr.append(self._random_date().strftime('%Y-%m-%d'))
        # Случайная буква латинского алфавита
        arr.append(''.join(random.choices(string.ascii_letters, k=10)))
        # Случайная буква русского алфавита
        arr.append(''.join(random.choices([chr(i)
                                           for i in range(a, a + 64)], k=10)))
        # Случайное положительное целоичисленное число
        arr.append(str(random.randint(1, 100000000)))
        # Случайное положительное число с плавающей точкой  в диапазоне от 1 до 20
        arr.append(str(round(random.uniform(1, 20), 8))[:10])
        arr.append('')
        return arr

    def read_file(self, file):
        try:
            with open(os.path.join('files', file), 'r') as f:
                return f.readlines()
        except FileNotFoundError:
            return f'Файл не найден'

    def merge_files(self, for_del):
        # Список файлов в папке
        try:
            files = os.listdir('files')
        except FileNotFoundError:
            return f'Файлы для объединения отсутствуют'
        count = 0  # Для подсчета количества удаленных строк
        res = ''
        for file in files:
            lines = self.read_file(file)  # Читается файл
            with open(os.path.join('files', file), 'w') as output_file:
                # Проверяется, задан ли символ для удаления и, если он присутствует в строке, строку не перезаписываем
                for line in lines:
                    if for_del and for_del in line:
                        count += 1
                        continue
                    output_file.write(line)
                    res += line  # Собираем информацию для общего файла
            with open(os.path.join('', 'merged.txt'), 'w') as input_file:
                # Записываем объединенную информацию в общий файл
                input_file.write(res)
        return count

    def upload_to_db(self):
        # Соединение с сервером БД
        engine = create_engine(
            'postgresql://postgres:Hello@localhost:5432/postgres',
            echo=True)
        # Название столбцов для df
        columns = ['date', 'lat_let', 'rus_let', 'int_n', 'float_n']
        try:
            # Считывается файл в DataFrame
            df = pd.read_fwf('merged.txt', names=columns,
                             delimiter='||', engine='python',
                             chunksize=10000)
        except FileNotFoundError:
            return f'Информация для добавления в базу данных отсутствует'
        size = sum(1 for row in open('merged.txt', 'r'))
        count = 0  # Для подсчета количества импортированных строк
        for line in df:
            # Запись в базу данных
            line.to_sql(
                'all_data',
                engine,
                index=False,
                if_exists='replace',
                method='multi'
            )
            chunk = len(line.index)  # Количество строк, которое записывается в бд
            count += chunk
            size -= chunk  # Количество оставшихся строк для записи
            print(f'Импортировано {count} строк, осталось импортировать {size} строк')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-create', action='store_true',
                        help='Создать новые файлы')
    parser.add_argument('-files', type=int, default=100,
                        help='Количество файлов.'
                             ' По умолчанию 100 файлов')
    parser.add_argument('-strings', type=int, default=100000,
                        help='Количество строк. '
                             'По умолчанию 100 000 строк')
    parser.add_argument('-delete', type=str,
                        help='Введите символ, '
                             'который необходимо удалить из файлов.')
    parser.add_argument('-sql', action='store_true',
                        help=f'Используйте флаг -sql, '
                        f'если хотите импортировать в базу данных')
    args = parser.parse_args()
    file = EYFile(args.files, args.strings)
    if args.create:
        file.create_files()
        print(f'Создано {args.files} файлов '
              f'в которых содержится по {args.strings} строк')
    if args.delete:
        count = file.merge_files(args.delete)
        print(f'Объединение файлов прошло успешно. '
              f'Всего удалено {count} строк, содержащих "{args.delete}"')
    if args.sql:
        file.upload_to_db()
