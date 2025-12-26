// Инициализация реплика-сета (нужен для некоторых аггрегаций)
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "mongodb:27017" }]
});

// Ждем инициализации реплика-сета
sleep(5000);

// Создаем базу данных и пользователя
db = db.getSiblingDB('ozon_catalog');

db.createUser({
  user: 'app_user',
  pwd: 'app_password',
  roles: [
    { role: 'readWrite', db: 'ozon_catalog' },
    { role: 'dbAdmin', db: 'ozon_catalog' },
    { role: 'clusterMonitor', db: 'admin' }
  ]
});

// Создаем коллекции
db.createCollection('offers', {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["offer_id", "offer_name", "category"],
      properties: {
        offer_id: { bsonType: "string" },
        offer_name: { bsonType: "string" },
        category: {
          bsonType: "object",
          required: ["id", "path", "level"]
        }
      }
    }
  }
});

db.createCollection('categories');
db.createCollection('offer_stats');

print('=== MongoDB initialized ===');
print('- Replica set: rs0');
print('- Database: ozon_catalog');
print('- User: app_user (app_password)');