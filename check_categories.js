
// Показать статистику категорий
print("=".repeat(60));
print("СТАТИСТИКА КАТЕГОРИЙ В БАЗЕ ecom_catalog");
print("=".repeat(60));

use ecom_catalog;

// Общее количество
const total = db.categories.countDocuments();
print(`Всего категорий: ${total.toLocaleString()}`);

// Распределение по уровням
print("\nРаспределение по уровням:");
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
print("\nПримеры категорий разных уровней:");

// Уровень 1
const level1 = db.categories.findOne({level: 1});
if (level1) {
  print("\nКатегория уровня 1:");
  print(JSON.stringify(level1, null, 2));
}

// Уровень 3
const level3 = db.categories.findOne({level: 3});
if (level3) {
  print("\nКатегория уровня 3:");
  print(JSON.stringify(level3, null, 2));
}

// Уровень 8 (максимальный)
const level8 = db.categories.findOne({level: 8});
if (level8) {
  print("\nКатегория уровня 8:");
  print(JSON.stringify(level8, null, 2));
}

// Топ-3 категории по количеству товаров
print("\nТоп-3 категории по количеству товаров:");
const top3 = db.categories.find({})
  .sort({"metadata.total_products": -1})
  .limit(3)
  .toArray();

top3.forEach((cat, idx) => {
  print(`\n${idx + 1}. ${cat.name} (уровень ${cat.level})`);
  print(`   Товаров: ${cat.metadata.total_products.toLocaleString()}`);
  print(`   Путь: ${cat.path}`);
});
