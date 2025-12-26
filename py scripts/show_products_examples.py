from pymongo import MongoClient
import json
from datetime import datetime

def get_products_statistics():
    """Получение статистики по коллекции products"""
    print("=" * 80)
    print("СТАТИСТИКА КОЛЛЕКЦИИ PRODUCTS - ЗАДАНИЕ 1.3")
    print("=" * 80)
    
    client = MongoClient("mongodb://admin:mongoadmin@localhost:27017/")
    db = client.ecom_catalog
    
    try:
        # 1. Общее количество товаров
        total = db.products.count_documents({})
        print(f"\n1. ОБЩЕЕ КОЛИЧЕСТВО ТОВАРОВ: {total:,}")
        
        # 2. Пример документа для скриншота
        print("\n2. ПРИМЕР ДОКУМЕНТА ТОВАРА (для скриншота):")
        print("-" * 80)
        
        product = db.products.find_one({}, {
            "_id": 1, "partner": 1, "offer_id": 1, "name": 1, "type": 1,
            "category": 1, "created_at": 1, "updated_at": 1
        })
        
        if product:
            # Форматируем вывод
            formatted = {
                "_id": product["_id"],
                "partner": product["partner"],
                "offer_id": product["offer_id"],
                "name": product["name"],
                "type": product["type"],
                "category": {
                    "id": product["category"]["id"],
                    "name": product["category"]["name"],
                    "full_path": product["category"]["full_path"],
                    "breadcrumbs": product["category"]["breadcrumbs"]
                },
                "created_at": product["created_at"].isoformat() if hasattr(product["created_at"], 'isoformat') else str(product["created_at"]),
                "updated_at": product["updated_at"].isoformat() if hasattr(product["updated_at"], 'isoformat') else str(product["updated_at"])
            }
            print(json.dumps(formatted, indent=2, ensure_ascii=False))
        
        # 3. Топ-5 типов товаров (рассчитываем процент в Python)
        print("\n3. ТОП-5 ТИПОВ ТОВАРОВ ПО ЧАСТОТЕ ВСТРЕЧАЕМОСТИ:")
        print("-" * 80)
        
        pipeline = [
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        top_types = list(db.products.aggregate(pipeline))
        
        print(f"{'№':<3} {'Тип товара':<50} {'Количество':<12} {'Доля, %':<10}")
        print("-" * 80)
        for i, stat in enumerate(top_types, 1):
            type_name = stat['_id'][:48] + "..." if len(stat['_id']) > 48 else stat['_id']
            percentage = (stat['count'] / total * 100) if total > 0 else 0
            print(f"{i:<3} {type_name:<50} {stat['count']:<12,} {percentage:<10.3f}")
        
        # 4. Распределение товаров по партнерам
        print("\n4. РАСПРЕДЕЛЕНИЕ ТОВАРОВ ПО ПАРТНЕРАМ:")
        print("-" * 80)
        
        pipeline = [
            {"$group": {"_id": "$partner", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        partner_stats = list(db.products.aggregate(pipeline))
        
        print(f"{'Партнер':<20} {'Количество':<12} {'Доля, %':<10} {'Примечание':<30}")
        print("-" * 80)
        for stat in partner_stats:
            percentage = (stat['count'] / total * 100) if total > 0 else 0
            note = "Единственный продавец" if len(partner_stats) == 1 else ""
            print(f"{stat['_id']:<20} {stat['count']:<12,} {percentage:<10.2f} {note:<30}")
        
        # 5. Дополнительная статистика
        print("\n5. ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА:")
        print("-" * 80)
        
        # Количество уникальных типов
        unique_types = len(db.products.distinct("type"))
        print(f"Уникальных типов товаров: {unique_types:,}")
        
        # Количество уникальных категорий в товарах
        unique_categories = len(db.products.distinct("category.id"))
        print(f"Уникальных категорий в товарах: {unique_categories:,}")
        
        # Среднее количество уровней категорий
        pipeline = [
            {"$addFields": {"category_level": {"$size": "$category.breadcrumbs"}}},
            {"$group": {"_id": None, "avg_level": {"$avg": "$category_level"}}}
        ]
        
        try:
            avg_level_result = list(db.products.aggregate(pipeline))
            if avg_level_result:
                print(f"Среднее количество уровней категорий: {avg_level_result[0]['avg_level']:.2f}")
        except:
            pass
        
        # 6. Сохранение отчета
        print("\n6. СОХРАНЕНИЕ ИТОГОВОГО ОТЧЕТА...")
        
        report_data = {
            "report_generated": datetime.now().isoformat(),
            "database": "ecom_catalog",
            "collection": "products",
            "statistics": {
                "total_products": total,
                "top_5_product_types": [
                    {
                        "type": stat['_id'],
                        "count": stat['count'],
                        "percentage": (stat['count'] / total * 100) if total > 0 else 0
                    } for stat in top_types
                ],
                "partner_distribution": [
                    {
                        "partner": stat['_id'],
                        "count": stat['count'],
                        "percentage": (stat['count'] / total * 100) if total > 0 else 0
                    } for stat in partner_stats
                ],
                "additional_stats": {
                    "unique_types": unique_types,
                    "unique_categories": unique_categories
                }
            },
            "example_document": product
        }
        
        report_file = "task_1_3_final_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, default=str, ensure_ascii=False)
        
        print(f"✓ Отчет сохранен в файл: {report_file}")
        
        print("\n" + "=" * 80)
        print("✅ ЗАДАНИЕ 1.3 ВЫПОЛНЕНО!")
        print("=" * 80)
        print("\nДЛЯ ОТЧЕТА ПОТРЕБУЮТСЯ:")
        print("1. Скриншот примера документа (выше)")
        print("2. Статистика:")
        print(f"   - Общее количество товаров: {total:,}")
        print("   - Топ-5 типов товаров (см. таблицу выше)")
        print("   - Распределение по партнерам (см. таблицу выше)")
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    get_products_statistics()