import pandas as pd
from pymongo import MongoClient, InsertOne
from datetime import datetime
import time
from pathlib import Path
import sys

def load_ozon_data():
    """Загрузка данных Ozon в MongoDB"""
    print("=" * 70)
    print("ЗАГРУЗКА ДАННЫХ OZON В MONGODB")
    print("=" * 70)
    
    # Пути
    base_path = Path("D:/study_ITMO/Курс СУБД/practice_06_mongodb")
    data_path = base_path / "SnapShotForMongoDB"
    parquet_file = data_path / "ozon_inference_2025_10_17_offers_2025_10_17.pq"
    
    if not parquet_file.exists():
        print(f"✗ Файл не найден: {parquet_file}")
        print("Содержимое папки SnapShotForMongoDB:")
        for item in data_path.iterdir():
            print(f"  - {item.name}")
        return
    
    print(f"Файл: {parquet_file}")
    print(f"Размер: {parquet_file.stat().st_size / (1024**2):.1f} MB")
    
    try:
        # 1. Проверяем Python окружение
        print("\n1. Проверка Python окружения...")
        print(f"Python версия: {sys.version}")
        print(f"pandas версия: {pd.__version__}")
        
        # 2. Подключение к MongoDB
        print("\n2. Подключение к MongoDB...")
        client = MongoClient(
            "mongodb://admin:mongoadmin@localhost:27017/",
            serverSelectionTimeoutMS=10000
        )
        client.admin.command('ping')
        
        # Используем базу ozon_catalog
        db = client.ozon_catalog
        
        # Очищаем коллекцию
        db.offers.drop()
        print("✓ Коллекция 'offers' очищена")
        
        # 3. Чтение данных с правильным методом
        print("\n3. Чтение Parquet файла...")
        
        # Способ 1: Читаем весь файл
        print("  Чтение данных (это может занять несколько минут)...")
        start_time = time.time()
        
        # Используем правильный метод чтения
        df = pd.read_parquet(parquet_file)
        
        read_time = time.time() - start_time
        print(f"✓ Файл прочитан за {read_time:.1f} секунд")
        print(f"  Строк: {len(df):,}")
        print(f"  Колонок: {len(df.columns)}")
        print(f"  Поля: {list(df.columns)}")
        
        # Показываем пример данных
        print("\n  Пример данных (первые 3 строки):")
        print(df.head(3).to_string())
        
        # 4. Загрузка в MongoDB (пачками для оптимизации)
        print("\n4. Загрузка данных в MongoDB...")
        
        batch_size = 10000
        total_rows = len(df)
        operations = []
        inserted_count = 0
        
        print(f"  Всего строк для обработки: {total_rows:,}")
        print(f"  Размер батча: {batch_size}")
        print(f"  Примерное количество батчей: {total_rows // batch_size + 1}")
        
        start_load_time = time.time()
        
        for idx, row in df.iterrows():
            try:
                # Создаем документ товара
                category_path = str(row['Category_FullPathName'])
                offer_doc = {
                    "_id": str(row['Offer_ID']),
                    "offer_id": str(row['Offer_ID']),
                    "name": str(row['Offer_Name']),
                    "offer_type": str(row['Offer_Type']),
                    "partner": str(row['Partner_Name']),
                    "category": {
                        "id": str(row['Category_ID']),
                        "path": category_path,
                        "level": len(category_path.split('\\')) if category_path else 1,
                        "parts": category_path.split('\\')
                    },
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
                
                operations.append(InsertOne(offer_doc))
                inserted_count += 1
                
                # Вставляем пачками
                if len(operations) >= batch_size:
                    result = db.offers.bulk_write(operations, ordered=False)
                    operations = []
                    
                    # Показываем прогресс
                    percent = (idx + 1) / total_rows * 100
                    elapsed = time.time() - start_load_time
                    speed = (idx + 1) / elapsed if elapsed > 0 else 0
                    
                    print(f"  Обработано: {idx + 1:,}/{total_rows:,} ({percent:.1f}%)")
                    print(f"  Скорость: {speed:.0f} записей/сек")
                    
            except Exception as e:
                # Пропускаем ошибки в отдельных строках
                print(f"  Ошибка в строке {idx}: {e}")
                continue
        
        # Вставляем оставшиеся документы
        if operations:
            result = db.offers.bulk_write(operations, ordered=False)
        
        total_load_time = time.time() - start_load_time
        print(f"✓ Загрузка завершена за {total_load_time:.1f} секунд")
        print(f"  Всего загружено: {inserted_count:,} документов")
        print(f"  Средняя скорость: {inserted_count/total_load_time:.0f} записей/сек")
        
        # 5. Создание индексов
        print("\n5. Создание индексов...")
        
        # Создаем основные индексы
        indexes = [
            {"field": "offer_id", "unique": True},
            {"field": "category.id"},
            {"field": "category.path"},
            {"field": "partner"},
            {"field": "offer_type"},
            {"field": "name", "type": "text"}  # Текстовый индекс для поиска
        ]
        
        for idx_info in indexes:
            field = idx_info["field"]
            unique = idx_info.get("unique", False)
            
            if idx_info.get("type") == "text":
                db.offers.create_index([(field, "text")])
                print(f"  ✓ Текстовый индекс создан: {field}")
            else:
                db.offers.create_index([(field, 1)], unique=unique)
                print(f"  ✓ Индекс создан: {field}" + (" (уникальный)" if unique else ""))
        
        # 6. Статистика и проверка
        print("\n6. Статистика базы данных:")
        
        total_docs = db.offers.count_documents({})
        print(f"  Всего документов в коллекции: {total_docs:,}")
        
        # Размер коллекции
        stats = db.command("collstats", "offers")
        print(f"  Размер коллекции: {stats['size'] / (1024**2):.1f} MB")
        print(f"  Размер индексов: {stats['indexSize'] / (1024**2):.1f} MB")
        
        # 7. Примеры запросов
        print("\n7. Примеры агрегационных запросов:")
        
        # Топ-5 категорий по количеству товаров
        print("\n  Топ-5 категорий:")
        pipeline = [
            {"$group": {"_id": "$category.path", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        results = list(db.offers.aggregate(pipeline))
        for i, item in enumerate(results, 1):
            cat_name = item['_id'][:50] + "..." if len(item['_id']) > 50 else item['_id']
            print(f"    {i}. {cat_name}: {item['count']:,}")
        
        # Распределение по типам товаров
        print("\n  Распределение по типам товаров (топ-5):")
        pipeline = [
            {"$group": {"_id": "$offer_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        results = list(db.offers.aggregate(pipeline))
        for item in results:
            type_name = item['_id'][:30] + "..." if len(item['_id']) > 30 else item['_id']
            print(f"    {type_name}: {item['count']:,}")
        
        # 8. Сохранение отчета
        print("\n8. Создание отчета...")
        
        report = {
            "load_timestamp": datetime.utcnow().isoformat(),
            "source_file": str(parquet_file.name),
            "file_size_mb": parquet_file.stat().st_size / (1024**2),
            "total_rows": total_rows,
            "loaded_documents": inserted_count,
            "load_time_seconds": total_load_time,
            "load_speed_records_per_second": inserted_count / total_load_time if total_load_time > 0 else 0,
            "database": "ozon_catalog",
            "collection": "offers",
            "indexes": [idx["field"] for idx in indexes],
            "sample_queries": [
                {"description": "Топ категорий", "pipeline": pipeline[:3]}
            ]
        }
        
        import json
        report_file = base_path / "load_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"✓ Отчет сохранен: {report_file}")
        
        print("\n" + "=" * 70)
        print("✅ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ В MONGODB!")
        print("=" * 70)
        
        print("\nИНСТРУКЦИЯ ДЛЯ ПРОВЕРКИ:")
        print("\n1. Откройте MongoDB Compass")
        print("   Подключитесь к: mongodb://admin:mongoadmin@localhost:27017")
        print("   База данных: ozon_catalog")
        print("   Коллекция: offers")
        
        print("\n2. Примеры запросов в MongoDB Compass:")
        print("   • Найти все товары в категории:")
        print('     {"category.path": {"$regex": "Строительство"}}')
        print("   • Товары конкретного типа:")
        print('     {"offer_type": "Лекарственное средство безрецептурное"}')
        print("   • Поиск по названию:")
        print('     {"$text": {"$search": "смартфон"}}')
        
        print("\n3. Проверьте количество документов:")
        print('   db.offers.countDocuments({})')
        
        print("\n4. Для работы с данными из Python:")
        print("""
   from pymongo import MongoClient
   client = MongoClient("mongodb://admin:mongoadmin@localhost:27017/")
   db = client.ozon_catalog
   offers = db.offers
   
   # Пример запроса
   for offer in offers.find({"category.level": 3}).limit(5):
       print(offer["name"])
        """)
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_ozon_data()