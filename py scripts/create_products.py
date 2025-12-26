import pandas as pd
from pymongo import MongoClient, InsertOne
from datetime import datetime
from pathlib import Path
import json
import sys

def create_products_collection():
    """Создание коллекции товаров с денормализованными категориями"""
    print("=" * 70)
    print("СОЗДАНИЕ КОЛЛЕКЦИИ ТОВАРОВ (products)")
    print("=" * 70)
    
    # Пути
    base_path = Path("D:/study_ITMO/Курс СУБД/practice_06_mongodb")
    data_path = base_path / "SnapShotForMongoDB"
    parquet_file = data_path / "ozon_inference_2025_10_17_offers_2025_10_17.pq"
    
    if not parquet_file.exists():
        print(f"✗ Файл не найден: {parquet_file}")
        return
    
    print(f"Чтение файла: {parquet_file}")
    
    try:
        # 1. Подключение к MongoDB
        print("\n1. Подключение к MongoDB...")
        client = MongoClient("mongodb://admin:mongoadmin@localhost:27017/")
        client.admin.command('ping')
        
        # Используем базу ecom_catalog
        db = client.ecom_catalog
        
        # Удаляем старую коллекцию products если существует
        db.products.drop()
        print("✓ Коллекция products очищена")
        
        # 2. Чтение данных
        print("\n2. Чтение Parquet файла...")
        start_time = datetime.now()
        
        # Читаем все необходимые колонки
        df = pd.read_parquet(parquet_file)
        
        read_time = (datetime.now() - start_time).total_seconds()
        print(f"✓ Файл прочитан за {read_time:.1f} секунд")
        print(f"  Строк: {len(df):,}")
        
        # 3. Подготовка данных для товаров
        print("\n3. Обработка товаров...")
        
        total_rows = len(df)
        products_list = []
        batch_size = 10000
        processed = 0
        
        for idx, row in df.iterrows():
            try:
                # Базовые поля
                partner = str(row['Partner_Name'])
                offer_id = str(row['Offer_ID'])
                
                # Создаем уникальный ID: "partner_offerid"
                doc_id = f"{partner}_{offer_id}"
                
                # Обрабатываем категорию
                category_path = str(row['Category_FullPathName'])
                normalized_path = category_path.replace('\\', '/')
                path_parts = [part.strip() for part in normalized_path.split('/') if part.strip()]
                
                if not path_parts:
                    continue
                
                # Создаем breadcrumbs (навигационная цепочка)
                breadcrumbs = []
                for i, part in enumerate(path_parts, 1):
                    breadcrumbs.append({
                        "level": i,
                        "name": part
                    })
                
                # Создаем документ товара
                product_doc = {
                    "_id": doc_id,
                    "partner": partner,
                    "offer_id": offer_id,
                    "name": str(row['Offer_Name']),
                    "type": str(row['Offer_Type']),
                    "category": {
                        "id": str(row['Category_ID']),
                        "name": path_parts[-1],  # Последний элемент пути
                        "full_path": normalized_path,
                        "breadcrumbs": breadcrumbs
                    },
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
                
                products_list.append(product_doc)
                processed += 1
                
                # Вставляем пачками для экономии памяти
                if len(products_list) >= batch_size:
                    insert_batch(db, products_list, processed, total_rows)
                    products_list = []
                
                # Прогресс каждые 100k записей
                if processed % 100000 == 0:
                    percent = processed / total_rows * 100
                    print(f"  Обработано: {processed:,}/{total_rows:,} ({percent:.1f}%)")
                    
            except Exception as e:
                # print(f"  Ошибка обработки товара {idx}: {e}")
                continue
        
        # Вставляем оставшиеся документы
        if products_list:
            insert_batch(db, products_list, processed, total_rows)
        
        print(f"✓ Всего обработано товаров: {processed:,}")
        
        # 4. Создание индексов
        print("\n4. Создание индексов...")
        
        indexes = [
            {"field": "partner", "name": "idx_partner"},
            {"field": "offer_id", "name": "idx_offer_id", "unique": True},
            {"field": "type", "name": "idx_type"},
            {"field": "category.id", "name": "idx_category_id"},
            {"field": "category.full_path", "name": "idx_category_path"},
            {"field": "name", "name": "idx_name_text", "type": "text"},
            {"field": "created_at", "name": "idx_created_at"},
            {"field": "updated_at", "name": "idx_updated_at"}
        ]
        
        for idx in indexes:
            try:
                if idx.get("type") == "text":
                    db.products.create_index([(idx["field"], "text")], name=idx["name"])
                else:
                    db.products.create_index([(idx["field"], 1)], name=idx["name"], 
                                            unique=idx.get("unique", False))
                print(f"  ✓ Индекс создан: {idx['name']}")
            except Exception as e:
                print(f"  ✗ Ошибка создания индекса {idx['name']}: {e}")
        
        # 5. Статистика (упрощенная, без ошибок в агрегациях)
        print("\n5. Сбор статистики...")
        
        # Общая статистика
        total_products = db.products.count_documents({})
        print(f"  Всего товаров в базе: {total_products:,}")
        
        # Топ-5 типов товаров (упрощенный запрос)
        print(f"\n  ТОП-5 ТИПОВ ТОВАРОВ:")
        top_types_cursor = db.products.aggregate([
            {
                "$group": {
                    "_id": "$type",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ])
        
        top_types = list(top_types_cursor)
        for i, type_stat in enumerate(top_types, 1):
            type_name = type_stat['_id'][:50] + "..." if len(type_stat['_id']) > 50 else type_stat['_id']
            percentage = (type_stat['count'] / total_products * 100) if total_products > 0 else 0
            print(f"    {i}. {type_name}")
            print(f"       Количество: {type_stat['count']:,}")
            print(f"       Доля: {percentage:.3f}%")
        
        # Распределение по партнерам
        print(f"\n  РАСПРЕДЕЛЕНИЕ ПО ПАРТНЕРАМ:")
        partner_stats_cursor = db.products.aggregate([
            {
                "$group": {
                    "_id": "$partner",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}}
        ])
        
        partner_stats = list(partner_stats_cursor)
        for stat in partner_stats:
            percentage = (stat['count'] / total_products * 100) if total_products > 0 else 0
            print(f"    Партнер: {stat['_id']}")
            print(f"       Товаров: {stat['count']:,} ({percentage:.1f}%)")
        
        # Распределение по уровням категорий
        print(f"\n  РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ КАТЕГОРИЙ:")
        level_stats_cursor = db.products.aggregate([
            {
                "$addFields": {
                    "category_level": {"$size": "$category.breadcrumbs"}
                }
            },
            {
                "$group": {
                    "_id": "$category_level",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ])
        
        level_stats = list(level_stats_cursor)
        for stat in level_stats:
            percentage = (stat['count'] / total_products * 100) if total_products > 0 else 0
            print(f"    Уровень {stat['_id']}: {stat['count']:,} товаров ({percentage:.1f}%)")
        
        # 6. Сохранение отчета
        print("\n6. Создание отчета...")
        
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "source_file": str(parquet_file.name),
            "total_products_created": total_products,
            "top_product_types": [
                {
                    "type": stat['_id'],
                    "count": stat['count'],
                    "percentage": (stat['count'] / total_products * 100) if total_products > 0 else 0
                } for stat in top_types
            ],
            "partner_distribution": [
                {
                    "partner": stat['_id'],
                    "count": stat['count'],
                    "percentage": (stat['count'] / total_products * 100) if total_products > 0 else 0
                } for stat in partner_stats
            ],
            "category_level_distribution": [
                {
                    "level": stat['_id'],
                    "count": stat['count'],
                    "percentage": (stat['count'] / total_products * 100) if total_products > 0 else 0
                } for stat in level_stats
            ]
        }
        
        # Сохраняем отчет
        report_file = base_path / "products_report_fixed.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"✓ Отчет сохранен: {report_file}")
        
        # 7. Пример документа для скриншота
        print("\n7. Подготовка примера документа для скриншота...")
        
        try:
            example_product = db.products.aggregate([
                {"$match": {"category.breadcrumbs": {"$size": 4}}},  # Берем товар с 4 уровнями категорий
                {"$project": {
                    "_id": 1,
                    "partner": 1,
                    "offer_id": 1,
                    "name": 1,
                    "type": 1,
                    "category": 1,
                    "created_at": 1,
                    "updated_at": 1
                }},
                {"$limit": 1}
            ]).next()
            
            example_file = base_path / "product_example_fixed.json"
            with open(example_file, "w", encoding="utf-8") as f:
                json.dump(example_product, f, indent=2, default=str)
            
            print(f"✓ Пример документа сохранен: {example_file}")
        except:
            # Если не нашли с 4 уровнями, берем первый
            example_product = db.products.find_one({}, {
                "_id": 1, "partner": 1, "offer_id": 1, "name": 1, "type": 1,
                "category": 1, "created_at": 1, "updated_at": 1
            })
            if example_product:
                example_file = base_path / "product_example_fixed.json"
                with open(example_file, "w", encoding="utf-8") as f:
                    json.dump(example_product, f, indent=2, default=str)
                print(f"✓ Пример документа сохранен: {example_file}")
        
        print("\n" + "=" * 70)
        print("✅ КОЛЛЕКЦИЯ PRODUCTS УСПЕШНО СОЗДАНА!")
        print("=" * 70)
        
        print("\nИНСТРУКЦИЯ ДЛЯ ПРОВЕРКИ:")
        print("\n1. Проверьте коллекцию в MongoDB:")
        print("   docker exec -it mongodb mongosh -u admin -p mongoadmin --authenticationDatabase admin")
        print("   use ecom_catalog")
        print("   db.products.countDocuments()")
        
        print("\n2. Для получения статистики выполните:")
        print("   // Топ-5 типов товаров")
        print('   db.products.aggregate([{$group: {_id: "$type", count: {$sum: 1}}}, {$sort: {count: -1}}, {$limit: 5}])')
        print('')
        print('   // Распределение по партнерам')
        print('   db.products.aggregate([{$group: {_id: "$partner", count: {$sum: 1}}}, {$sort: {count: -1}}])')
        
        client.close()
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()

def insert_batch(db, batch, processed, total):
    """Вставка батча документов"""
    try:
        db.products.insert_many(batch, ordered=False)
        percent = processed / total * 100
        print(f"  Вставлено: {processed:,}/{total:,} ({percent:.1f}%)")
    except Exception as e:
        print(f"  Ошибка вставки батча: {e}")
        # Пробуем вставить по одному
        success = 0
        for doc in batch:
            try:
                db.products.insert_one(doc)
                success += 1
            except:
                pass
        print(f"  Вставлено по одному: {success}/{len(batch)}")

if __name__ == "__main__":
    create_products_collection()