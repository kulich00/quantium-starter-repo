import pandas as pd
import glob
import os

def process_pink_morsels(input_f, output_f):

    all_data = []
    
    for file in input_f:
        try:
            # Чтение файла с автоматическим определением разделителя
            df = pd.read_csv(file, sep=None, engine='python')
            
            # Стандартизация названий колонок (регистронезависимая)
            df.columns = df.columns.str.strip().str.lower()
            
            # Проверка необходимых колонок с разными вариантами названий
            col_mapping = {
                'product': ['product', 'item', 'товар', 'продукт'],
                'quantity': ['quantity', 'количество', 'qty', 'amount'],
                'price': ['price', 'цена', 'стоимость'],
                'region': ['region', 'регион', 'area', 'zone']
            }
            
            # Поиск соответствий колонок
            found_cols = {}
            for standard_col, variants in col_mapping.items():
                for variant in variants:
                    if variant in df.columns:
                        found_cols[standard_col] = variant
                        break
            
            # Проверка, что все нужные колонки найдены
            if len(found_cols) != 4:
                missing = set(col_mapping.keys()) - set(found_cols.keys())
                print(f"Файл {os.path.basename(file)} пропущен. Не найдены колонки: {missing}")
                continue
            
            # Переименование колонок в стандартные названия
            df = df.rename(columns={v: k for k, v in found_cols.items()})
            
            # Фильтрация Pink Morsels (регистронезависимая)
            pink_data = df[df['product'].str.strip().str.lower() == 'pink morsel'].copy()
            
            if pink_data.empty:
                print(f"Файл {os.path.basename(file)} не содержит данных Pink Morsels")
                continue
            
            # Обработка цены (удаление символов валют и пробелов)
            pink_data['price'] = (
                pink_data['price']
                .astype(str)
                .str.replace(r'[^\d.]', '', regex=True)
                .astype(float)
            )
            
            # Расчет продаж
            pink_data['sales'] = pink_data['quantity'] * pink_data['price']
            
            # Выбор нужных колонок
            result_cols = pink_data[['sales', 'region', 'price']]
            all_data.append(result_cols)
            
            print(f"Обработан файл {os.path.basename(file)} | Найдено записей: {len(result_cols)}")
            
        except Exception as e:
            print(f"Ошибка при обработке файла {os.path.basename(file)}: {str(e)}")
            continue
    
    if not all_data:
        print("Не найдено подходящих данных для обработки")
        return False
    
    # Объединение всех данных
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Сохранение результата
    final_df.to_csv(output_f, index=False)
    print(f"\nРезультат сохранен в {output_f}")
    print(f"Всего записей: {len(final_df)}")
    return True

if __name__ == "__main__":
    # Настройки
    input_f = "quantium-starter-repo/data"  # Папка с файлами (текущая директория)
    pattern = "*.csv"  # Шаблон для поиска файлов
    output_f = "quantium-starter-repo/formatted_data.csv"
    
    # Поиск файлов
    input_files = glob.glob(os.path.join(input_f, pattern))
    
    if not input_files:
        print(f"Не найдено CSV-файлов по шаблону '{pattern}' в директории '{input_f}'")
    else:
        print(f"Найдено файлов для обработки: {len(input_files)}")
        process_pink_morsels(input_files, output_f)