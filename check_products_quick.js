// Быстрая проверка коллекции products
print("=".repeat(80));
print("БЫСТРАЯ ПРОВЕРКА КОЛЛЕКЦИИ PRODUCTS - ЗАДАНИЕ 1.3");
print("=".repeat(80));

use ecom_catalog;

// 1. Общее количество
const total = db.products.countDocuments();
print(`1. Общее количество товаров: ${total.toLocaleString()}`);

// 2. Пример документа для скриншота
print("\n2. Пример документа товара для скриншота:");
const exampleProduct = db.products.findOne(
    {},
    {
        _id: 1,
        partner: 1,
        offer_id: 1,
        name: 1,
        type: 1,
        category: 1,
        created_at: 1,
        updated_at: 1
    }
);
printjson(exampleProduct);

// 3. Топ-5 типов товаров
print("\n3. Топ-5 типов товаров по частоте встречаемости:");
const topTypes = db.products.aggregate([
    {
        $group: {
            _id: "$type",
            count: { $sum: 1 },
            percentage: {
                $multiply: [
                    { $divide: [{ $sum: 1 }, total] },
                    100
                ]
            }
        }
    },
    { $sort: { count: -1 } },
    { $limit: 5 }
]).toArray();

print("№  Тип товара" + " ".repeat(40) + "Количество   Доля, %");
print("-".repeat(80));
topTypes.forEach((item, idx) => {
    const typeName = item._id.length > 50 ? item._id.substring(0, 47) + "..." : item._id;
    const paddedName = typeName.padEnd(50, ' ');
    print(`${idx + 1}. ${paddedName} ${item.count.toString().padStart(10, ' ')}     ${item.percentage.toFixed(3)}`);
});

// 4. Распределение по партнерам
print("\n4. Распределение товаров по партнерам:");
const partners = db.products.aggregate([
    {
        $group: {
            _id: "$partner",
            count: { $sum: 1 },
            percentage: {
                $multiply: [
                    { $divide: [{ $sum: 1 }, total] },
                    100
                ]
            }
        }
    },
    { $sort: { count: -1 } }
]).toArray();

print("Партнер".padEnd(20) + "Количество".padEnd(15) + "Доля, %");
print("-".repeat(50));
partners.forEach(partner => {
    print(`${partner._id.padEnd(20)} ${partner.count.toString().padStart(12, ' ')}     ${partner.percentage.toFixed(2)}`);
});

// 5. Проверка структуры
print("\n5. Проверка структуры документов:");
const sample = db.products.findOne();
const requiredFields = [
    "_id", "partner", "offer_id", "name", "type", 
    "category", "created_at", "updated_at"
];

const categoryFields = ["id", "name", "full_path", "breadcrumbs"];

let allOk = true;

// Проверка основных полей
requiredFields.forEach(field => {
    const exists = sample.hasOwnProperty(field);
    print(`  ${field}: ${exists ? '✓' : '✗'}`);
    if (!exists) allOk = false;
});

// Проверка полей категории
if (sample.category) {
    print("\n  Поля категории:");
    categoryFields.forEach(field => {
        const exists = sample.category.hasOwnProperty(field);
        print(`    ${field}: ${exists ? '✓' : '✗'}`);
        if (!exists) allOk = false;
    });
}

print("\n" + "=".repeat(80));
if (allOk) {
    print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!");
} else {
    print("⚠ ЕСТЬ ПРОБЛЕМЫ СО СТРУКТУРОЙ ДОКУМЕНТОВ");
}
print("=".repeat(80));