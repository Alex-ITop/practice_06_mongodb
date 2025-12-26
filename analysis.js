// Анализ данных Ozon для задания 1.1
print("=".repeat(70));
print("АНАЛИЗ ИСХОДНЫХ ДАННЫХ OZON - ЗАДАНИЕ 1.1");
print("=".repeat(70));

// Используем базу ozon_catalog
use ozon_catalog;

// 1. Базовые метрики
print("\n1. БАЗОВЫЕ МЕТРИКИ ДАННЫХ:");
const totalOffers = db.offers.countDocuments();
print(`   Всего товаров: ${totalOffers.toLocaleString()}`);

// 2. Уникальные категории
const uniqueCategories = db.offers.distinct("category.path").length;
print(`\n2. УНИКАЛЬНЫЕ КАТЕГОРИИ: ${uniqueCategories}`);

// 3. Глубина вложенности категорий
print(`\n3. ГЛУБИНА ВЛОЖЕННОСТИ КАТЕГОРИЙ:`);
const levels = db.offers.aggregate([
  {
    $group: {
      _id: null,
      max_level: { $max: "$category.level" },
      min_level: { $min: "$category.level" },
      avg_level: { $avg: "$category.level" }
    }
  }
]).toArray()[0];

print(`   Максимальная глубина: ${levels.max_level}`);
print(`   Минимальная глубина: ${levels.min_level}`);
print(`   Средняя глубина: ${levels.avg_level.toFixed(2)}`);

// Распределение по уровням
print(`\n   Распределение по уровням:`);
const levelDistribution = db.offers.aggregate([
  {
    $group: {
      _id: "$category.level",
      count: { $sum: 1 },
      percentage: {
        $multiply: [
          { $divide: [{ $sum: 1 }, totalOffers] },
          100
        ]
      }
    }
  },
  { $sort: { _id: 1 } }
]).toArray();

levelDistribution.forEach(item => {
  print(`   Уровень ${item._id}: ${item.count.toLocaleString()} товаров (${item.percentage.toFixed(1)}%)`);
});

// 4. Категории с наибольшим количеством товаров
print(`\n4. ТОП-5 КАТЕГОРИЙ ПО КОЛИЧЕСТВУ ТОВАРОВ:`);
const topCategories = db.offers.aggregate([
  {
    $group: {
      _id: "$category.path",
      total_offers: { $sum: 1 }
    }
  },
  { $sort: { total_offers: -1 } },
  { $limit: 5 }
]).toArray();

topCategories.forEach((cat, idx) => {
  const catName = cat._id.length > 50 ? cat._id.substring(0, 50) + "..." : cat._id;
  const percentage = (cat.total_offers / totalOffers * 100).toFixed(3);
  print(`   ${idx + 1}. ${catName}`);
  print(`      Товаров: ${cat.total_offers.toLocaleString()} (${percentage}%)`);
});

// 5. Анализ дубликатов Offer_ID
print(`\n5. АНАЛИЗ ДУБЛИКАТОВ OFFER_ID:`);
const duplicates = db.offers.aggregate([
  {
    $group: {
      _id: "$offer_id",
      count: { $sum: 1 },
      partners: { $addToSet: "$partner" }
    }
  },
  {
    $match: {
      count: { $gt: 1 }
    }
  },
  {
    $count: "total_duplicates"
  }
]).toArray();

if (duplicates.length > 0) {
  print(`   Товаров с дубликатами: ${duplicates[0].total_duplicates}`);
  
  // Покажем примеры дубликатов
  const sampleDuplicates = db.offers.aggregate([
    {
      $group: {
        _id: "$offer_id",
        count: { $sum: 1 },
        partners: { $addToSet: "$partner" }
      }
    },
    {
      $match: {
        count: { $gt: 1 }
      }
    },
    { $limit: 3 }
  ]).toArray();
  
  print(`   Примеры дубликатов:`);
  sampleDuplicates.forEach((dup, idx) => {
    print(`   ${idx + 1}. Offer_ID: ${dup._id}`);
    print(`      Количество дублей: ${dup.count}`);
    print(`      Продавцы: ${dup.partners.join(', ')}`);
  });
} else {
  print(`   Дубликатов не обнаружено`);
}

// 6. Уникальные типы товаров
print(`\n6. УНИКАЛЬНЫЕ ТИПЫ ТОВАРОВ (OFFER_TYPE):`);
const offerTypes = db.offers.distinct("offer_type").length;
print(`   Всего уникальных типов: ${offerTypes}`);

print(`\n   Топ-5 типов товаров:`);
const topTypes = db.offers.aggregate([
  {
    $group: {
      _id: "$offer_type",
      count: { $sum: 1 },
      percentage: {
        $multiply: [
          { $divide: [{ $sum: 1 }, totalOffers] },
          100
        ]
      }
    }
  },
  { $sort: { count: -1 } },
  { $limit: 5 }
]).toArray();

topTypes.forEach((type, idx) => {
  const typeName = type._id.length > 40 ? type._id.substring(0, 40) + "..." : type._id;
  print(`   ${idx + 1}. ${typeName}`);
  print(`      Количество: ${type.count.toLocaleString()} (${type.percentage.toFixed(3)}%)`);
});

// 7. Анализ продавцов
print(`\n7. АНАЛИЗ ПРОДАВЦОВ:`);
const partners = db.offers.distinct("partner");
print(`   Уникальных продавцов: ${partners.length}`);

print(`\n   Статистика по продавцам:`);
const partnerStats = db.offers.aggregate([
  {
    $group: {
      _id: "$partner",
      total_offers: { $sum: 1 },
      unique_categories: { $addToSet: "$category.path" },
      unique_offer_types: { $addToSet: "$offer_type" }
    }
  },
  {
    $project: {
      partner: "$_id",
      total_offers: 1,
      percentage: {
        $multiply: [
          { $divide: ["$total_offers", totalOffers] },
          100
        ]
      },
      category_count: { $size: "$unique_categories" },
      offer_type_count: { $size: "$unique_offer_types" },
      _id: 0
    }
  },
  { $sort: { total_offers: -1 } }
]).toArray();

partnerStats.forEach((partner, idx) => {
  print(`   ${idx + 1}. ${partner.partner}`);
  print(`      Товаров: ${partner.total_offers.toLocaleString()} (${partner.percentage.toFixed(1)}%)`);
  print(`      Категорий: ${partner.category_count}`);
  print(`      Типов товаров: ${partner.offer_type_count}`);
});

print("\n" + "=".repeat(70));
print("КРАТКАЯ АНАЛИТИЧЕСКАЯ СПРАВКА");
print("=".repeat(70));

print(`
1. ОБЪЕМ ДАННЫХ: ${totalOffers.toLocaleString()} товаров от ${partners.length} продавца(ов)
2. КАТЕГОРИИ: ${uniqueCategories} уникальных категорий, максимальная глубина ${levels.max_level} уровня
3. РАСПРЕДЕЛЕНИЕ: ${levelDistribution[1] ? levelDistribution[1].percentage.toFixed(1) : 0}% товаров в категориях уровня 2
4. ТИПЫ ТОВАРОВ: ${offerTypes} уникальных типов, топ-5 занимают ${topTypes.reduce((sum, t) => sum + t.percentage, 0).toFixed(2)}% объема
5. ДУБЛИКАТЫ: ${duplicates.length > 0 ? duplicates[0].total_duplicates + ' товаров имеют дубликаты' : 'дубликатов не обнаружено'}
`);

print("\n" + "=".repeat(70));
print("РЕКОМЕНДАЦИИ ПО СТРУКТУРЕ БАЗЫ ДАННЫХ");
print("=".repeat(70));

print(`
1. РАЗДЕЛЕНИЕ НА ДВЕ КОЛЛЕКЦИИ:
   - Коллекция "categories" для хранения уникальных категорий с метаданными
   - Коллекция "products" для товаров с денормализованными данными категорий

2. ПРЕИМУЩЕСТВА РАЗДЕЛЕНИЯ:
   • Нормализация данных категорий
   • Централизованное управление иерархией
   • Легкое обновление названий категорий
   • Оптимизация запросов по категориям

3. ПРЕИМУЩЕСТВА ДЕНОРМАЛИЗАЦИИ В "products":
   • Быстрый доступ к данным категории без JOIN
   • Оптимизация для операций чтения (основная нагрузка)
   • Упрощение запросов фильтрации по категориям
   • Кэширование часто используемых данных
`);

print("\n" + "=".repeat(70));
print("АНАЛИЗ ЗАВЕРШЕН");
print("=".repeat(70));