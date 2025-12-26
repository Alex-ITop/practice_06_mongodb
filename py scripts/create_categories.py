import pandas as pd
from pymongo import MongoClient, InsertOne
from datetime import datetime
from pathlib import Path
import hashlib
import json
import sys

def create_categories_collection():
    """Создание коллекции категорий из Parquet файла"""
    print("=" * 70)
    print("СОЗДАНИЕ КОЛЛЕКЦИИ КАТЕГОРИЙ (categories)")
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
        
        # Используем базу ecom_catalog как требуется в задании
        db = client.ecom_catalog
        
        # Удаляем старую коллекцию categories если существует
        db.categories.drop()
        print("✓ Коллекция categories очищена")
        
        # 2. Чтение данных
        print("\n2. Чтение Parquet файла...")
        start_time = datetime.now()
        
        # Читаем только необходимые колонки для экономии памяти
        columns = ['Partner_Name', 'Category_ID', 'Category_FullPathName', 'Offer_ID']
        df = pd.read_parquet(parquet_file, columns=columns)
        
        read_time = (datetime.now() - start_time).total_seconds()
        print(f"✓ Файл прочитан за {read_time:.1f} секунд")
        print(f"  Строк: {len(df):,}")
        
        # 3. Подготовка данных для категорий
        print("\n3. Обработка категорий...")
        
        # Группируем по уникальным комбинациям Partner + Category
        category_groups = df.groupby(['Partner_Name', 'Category_ID', 'Category_FullPathName'])
        
        # Создаем список для хранения документов категорий
        categories_dict = {}
        total_categories = len(category_groups)
        print(f"  Найдено уникальных комбинаций категорий: {total_categories:,}")
        
        processed = 0
        for (partner_name, category_id, category_path), group in category_groups:
            try:
                # Проверяем данные
                if pd.isna(category_path) or not isinstance(category_path, str):
                    continue
                
                # Нормализуем путь: заменяем \ на /
                normalized_path = category_path.replace('\\', '/')
                path_parts = [part.strip() for part in normalized_path.split('/') if part.strip()]
                
                if not path_parts:
                    continue
                
                # Создаем уникальный ID: "partner_categoryid"
                doc_id = f"{partner_name}_{category_id}"
                
                # Определяем уровень вложенности
                level = len(path_parts)
                
                # Определяем родительский путь
                parent_path = '/'.join(path_parts[:-1]) if level > 1 else None
                
                # Название категории - последний элемент пути
                category_name = path_parts[-1]
                
                # Количество товаров в категории
                total_products = len(group)
                
                # Создаем документ категории
                category_doc = {
                    "_id": doc_id,
                    "partner": partner_name,
                    "category_id": str(category_id),
                    "name": category_name,
                    "path": normalized_path,
                    "path_array": path_parts,
                    "level": level,
                    "parent_path": parent_path,
                    "metadata": {
                        "total_products": int(total_products),
                        "last_updated": datetime.utcnow()
                    }
                }
                
                categories_dict[doc_id] = category_doc
                processed += 1
                
                # Прогресс
                if processed % 1000 == 0:
                    percent = processed / total_categories * 100
                    print(f"  Обработано: {processed:,}/{total_categories:,} ({percent:.1f}%)")
                    
            except Exception as e:
                print(f"  Ошибка обработки категории {partner_name}/{category_id}: {e}")
                continue
        
        print(f"✓ Обработано категорий: {len(categories_dict):,}")
        
        # 4. Вставка категорий в MongoDB
        print("\n4. Вставка категорий в MongoDB...")
        
        # Преобразуем словарь в список
        categories_list = list(categories_dict.values())
        
        # Вставляем пачками по 1000
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(categories_list), batch_size):
            batch = categories_list[i:i + batch_size]
            
            try:
                result = db.categories.insert_many(batch, ordered=False)
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count
                
                percent = (i + len(batch)) / len(categories_list) * 100
                print(f"  Вставлено: {total_inserted:,}/{len(categories_list):,} ({percent:.1f}%)")
                
            except Exception as e:
                print(f"  Ошибка вставки батча {i//batch_size}: {e}")
                # Пробуем вставить по одному
                for doc in batch:
                    try:
                        db.categories.insert_one(doc)
                        total_inserted += 1
                    except:
                        pass
        
        print(f"✓ Всего вставлено категорий: {total_inserted:,}")
        
        # 5. Создание индексов
        print("\n5. Создание индексов...")
        
        indexes = [
            {"field": "partner", "name": "idx_partner"},
            {"field": "category_id", "name": "idx_category_id", "unique": False},
            {"field": "path", "name": "idx_path"},
            {"field": "level", "name": "idx_level"},
            {"field": "parent_path", "name": "idx_parent_path"},
            {"field": "metadata.total_products", "name": "idx_total_products"}
        ]
        
        for idx in indexes:
            try:
                db.categories.create_index([(idx["field"], 1)], name=idx["name"])
                print(f"  ✓ Индекс создан: {idx['name']}")
            except Exception as e:
                print(f"  ✗ Ошибка создания индекса {idx['name']}: {e}")
        
        # 6. Статистика
        print("\n6. Сбор статистики...")
        
        # Общая статистика
        total_categories_in_db = db.categories.count_documents({})
        print(f"  Всего категорий в базе: {total_categories_in_db:,}")
        
        # Распределение по уровням
        pipeline = [
            {
                "$group": {
                    "_id": "$level",
                    "count": {"$sum": 1},
                    "total_products_sum": {"$sum": "$metadata.total_products"},
                    "avg_products_per_category": {"$avg": "$metadata.total_products"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        level_stats = list(db.categories.aggregate(pipeline))
        
        print(f"\n  РАСПРЕДЕЛЕНИЕ КАТЕГОРИЙ ПО УРОВНЯМ:")
        for stat in level_stats:
            print(f"    Уровень {stat['_id']}: {stat['count']:,} категорий")
            print(f"      Товаров всего: {stat['total_products_sum']:,}")
            print(f"      Среднее товаров на категорию: {stat['avg_products_per_category']:.1f}")
        
        # Топ-5 категорий по количеству товаров
        print(f"\n  ТОП-5 КАТЕГОРИЙ ПО КОЛИЧЕСТВУ ТОВАРОВ:")
        top_categories = db.categories.find(
            {},
            {"_id": 1, "name": 1, "level": 1, "path": 1, "metadata.total_products": 1}
        ).sort("metadata.total_products", -1).limit(5)
        
        for i, cat in enumerate(top_categories, 1):
            cat_name = cat['name'][:30] + "..." if len(cat['name']) > 30 else cat['name']
            print(f"    {i}. {cat_name} (уровень {cat['level']})")
            print(f"       Товаров: {cat['metadata']['total_products']:,}")
            print(f"       Путь: {cat['path'][:50]}..." if len(cat['path']) > 50 else f"       Путь: {cat['path']}")
        
        # 7. Сохранение отчета
        print("\n7. Создание отчета...")
        
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "source_file": str(parquet_file.name),
            "total_categories_created": total_categories_in_db,
            "level_distribution": {str(stat["_id"]): stat["count"] for stat in level_stats},
            "indexes_created": [idx["name"] for idx in indexes],
            "sample_categories": []
        }
        
        # Добавляем примеры категорий для отчета
        sample_categories = list(db.categories.aggregate([
            {"$match": {"level": {"$in": [1, 3, 5, 8]}}},
            {"$sample": {"size": 2}},
            {"$project": {
                "_id": 1,
                "name": 1,
                "level": 1,
                "path": 1,
                "parent_path": 1,
                "metadata.total_products": 1
            }}
        ]))
        
        for cat in sample_categories:
            report["sample_categories"].append({
                "_id": cat["_id"],
                "name": cat["name"],
                "level": cat["level"],
                "path": cat["path"],
                "parent_path": cat.get("parent_path"),
                "total_products": cat["metadata"]["total_products"]
            })
        
        # Сохраняем отчет
        report_file = base_path / "categories_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"✓ Отчет сохранен: {report_file}")
        
        # 8. Создание скрипта для просмотра в mongosh
        print("\n8. Создание скриптов для проверки...")
        
        # Скрипт для mongosh
        mongosh_script = """
// Показать статистику категорий
print("=".repeat(60));
print("СТАТИСТИКА КАТЕГОРИЙ В БАЗЕ ecom_catalog");
print("=".repeat(60));

use ecom_catalog;

// Общее количество
const total = db.categories.countDocuments();
print(`Всего категорий: ${total.toLocaleString()}`);

// Распределение по уровням
print("\\nРаспределение по уровням:");
const levels = db.categories.aggregate([
  {
    $group: {
      _id: "$level",
      count: { $sum: 1 },
      percentage: {
        $multiply: [
          { $divide: [{ $sum: 1 }, total] },
          100
        ]
      }
    }
  },
  { $sort: { _id: 1 } }
]).toArray();

levels.forEach(level => {
  print(`  Уровень ${level._id}: ${level.count.toLocaleString()} категорий (${level.percentage.toFixed(1)}%)`);
});

// Примеры категорий разных уровней
print("\\nПримеры категорий разных уровней:");

// Уровень 1
const level1 = db.categories.findOne({level: 1});
if (level1) {
  print("\\nКатегория уровня 1:");
  print(JSON.stringify(level1, null, 2));
}

// Уровень 3
const level3 = db.categories.findOne({level: 3});
if (level3) {
  print("\\nКатегория уровня 3:");
  print(JSON.stringify(level3, null, 2));
}

// Уровень 8 (максимальный)
const level8 = db.categories.findOne({level: 8});
if (level8) {
  print("\\nКатегория уровня 8:");
  print(JSON.stringify(level8, null, 2));
}

// Топ-3 категории по количеству товаров
print("\\nТоп-3 категории по количеству товаров:");
const top3 = db.categories.find({})
  .sort({"metadata.total_products": -1})
  .limit(3)
  .toArray();

top3.forEach((cat, idx) => {
  print(`\\n${idx + 1}. ${cat.name} (уровень ${cat.level})`);
  print(`   Товаров: ${cat.metadata.total_products.toLocaleString()}`);
  print(`   Путь: ${cat.path}`);
});
"""
        
        mongosh_file = base_path / "check_categories.js"
        with open(mongosh_file, "w", encoding="utf-8") as f:
            f.write(mongosh_script)
        
        print(f"✓ Скрипт для проверки создан: {mongosh_file}")
        
        print("\n" + "=" * 70)
        print("✅ КОЛЛЕКЦИЯ CATEGORIES УСПЕШНО СОЗДАНА!")
        print("=" * 70)
        
        print("\nИНСТРУКЦИЯ ДЛЯ ПРОВЕРКИ:")
        print("\n1. Проверьте коллекцию в MongoDB:")
        print("   docker exec -it mongodb mongosh -u admin -p mongoadmin --authenticationDatabase admin")
        print("   use ecom_catalog")
        print("   show collections")
        print("   db.categories.countDocuments()")
        
        print("\n2. Запустите скрипт проверки:")
        print(f"   docker exec mongodb mongosh -u admin -p mongoadmin --authenticationDatabase admin --file {mongosh_file.name}")
        
        print("\n3. Примеры запросов:")
        print('   // Категории уровня 1')
        print('   db.categories.find({level: 1}).limit(3).pretty()')
        print('')
        print('   // Категории с максимальным количеством товаров')
        print('   db.categories.find().sort({"metadata.total_products": -1}).limit(3).pretty()')
        print('')
        print('   // Поиск по пути')
        print('   db.categories.find({path: /Строительство/}).limit(3).pretty()')
        
        print("\n4. Для задания:")
        print("   - Скриншоты документов с разным уровнем вложенности")
        print("   - Статистика распределения по уровням готова")
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_categories_collection()