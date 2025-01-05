
// 1. Tìm tên các cột cũng như datatypes trong collection
db.SeminarCollection.aggregate([
  { $project: { fields: { $objectToArray: "$$ROOT" } } },
  { $unwind: "$fields" },
  { $group: { _id: null, uniqueFields: { $addToSet: "$fields.k" } } }
])

// 2. Count null values
db.SeminarCollection.aggregate([
  { $project: {
      fields: { $objectToArray: "$$ROOT" }
    }
  },
  { $unwind: "$fields"},
  { $group: {
      _id: "$fields.k",
      nullCount: {
        $sum: { $cond: [{ $eq: ["$fields.v", null] }, 1, 0] }
      },
      notExistCount: {
        $sum: { $cond: [{ $not: ["$fields.v"] }, 1, 0] }
      }
    }},
  { $sort: { _id: 1 }}
])

// 3. count distinct values 
db.SeminarCollection.aggregate([
  { $group: {
        _id: {
            time_stamp: "$time_stamp",
            ip: "$ip",
            user_agent: "$user_agent",
            resolution: "$resolution",
            user_id_db: "$user_id_db",
            device_id: "$device_id",
            api_version: "$api_version",
            store_id: "$store_id",
            local_time: "$local_time",
            show_recommendation: "$show_recommendation",
            current_url: "$current_url",
            referrer_url: "$referrer_url",
            email_address: "$email_address",
            recommendation: "$recommendation",
            utm_source: "$utm_source",
            utm_medium: "$utm_medium",
            collection: "$collection",
            product_id: "$product_id",
            option: "$option",
            order_id: "$order_id",
            cart_products: "$cart_products",
            cat_id: "$cat_id",
            collect_id: "$collect_id",
            viewing_product_id: "$viewing_product_id",
            recommendation_product_id: "$recommendation_product_id",
            recommendation_clicked_position: "$recommendation_clicked_position",
            price: "$price",
            currency: "$currency",
            is_paypal: "$is_paypal",
            key_search: "$key_search",
            recommendation_product_position: "$recommendation_product_position"
          },
          ids: { $push: "$_id" },
          count: { $sum: 1 }
      }
  },
  { $match: { count: { $gt: 1 } }}
// Removing duplicate rows
]).forEach(doc => {
  db.collection.deleteMany({
      _id: { $in: doc.ids.slice(1) } 
  });
});

// 4. Check data types consistency
db.SeminarCollection.aggregate([
  { $project: {
      price: 1, 
      priceType: { $type: "$price" },
      priceFormat: {
        $cond: [
          { $regexMatch: { input: { $toString: "$price" }, regex: /^[0-9]+\.[0-9]{2}$/ } }, 
          "decimal-dot",
          { $cond: [
            { $regexMatch: { input: { $toString: "$price" }, regex: /^[0-9]+,[0-9]{2}$/ } }, 
            "decimal-comma",
            "other"
          ] }
        ]}}
  },
  { $group: {
      _id: { type: "$priceType", format: "$priceFormat" }, 
      count: { $sum: 1 } // Đếm số lượng mỗi kiểu
    }
  },
  { $sort: { "_id.type": 1, "_id.format": 1 } }
])

// 5. Data Completeness
db.SeminarCollection.countDocuments();

// 6. Dữ liệu thu thập từ ngày nào đến ngày nào
db.SeminarCollection.aggregate([
  { $group: {
      _id: null,
      firstTimestamp: { $min: "$time_stamp" },
      lastTimestamp: { $max: "$time_stamp" },
      totalRecords: { $sum: 1 }
    }
  }
]);

// 7. Kiểm tra dữ liệu loại tiền tệ (currency)
db.SeminarCollection.aggregate([
  { $group: {
        _id: "$currency",
        count: { $sum: 1 }
      }
  },
  { $sort: {
        count: -1
      }}
// 1. Tìm tên các cột cũng như datatypes trong collection
db.SeminarCollection.aggregate([
  { $project: { fields: { $objectToArray: "$$ROOT" } } },
  { $unwind: "$fields" },
  { $group: { _id: null, uniqueFields: { $addToSet: "$fields.k" } } }
])

// 2. Count null values
db.SeminarCollection.aggregate([
  { $project: {
      fields: { $objectToArray: "$$ROOT" }
    }
  },
  { $unwind: "$fields"},
  { $group: {
      _id: "$fields.k",
      nullCount: {
        $sum: { $cond: [{ $eq: ["$fields.v", null] }, 1, 0] }
      },
      notExistCount: {
        $sum: { $cond: [{ $not: ["$fields.v"] }, 1, 0] }
      }
    }},
  { $sort: { _id: 1 }}
])

// 3. count distinct values 
db.SeminarCollection.aggregate([
  { $group: {
        _id: {
            time_stamp: "$time_stamp",
            ip: "$ip",
            user_agent: "$user_agent",
            resolution: "$resolution",
            user_id_db: "$user_id_db",
            device_id: "$device_id",
            api_version: "$api_version",
            store_id: "$store_id",
            local_time: "$local_time",
            show_recommendation: "$show_recommendation",
            current_url: "$current_url",
            referrer_url: "$referrer_url",
            email_address: "$email_address",
            recommendation: "$recommendation",
            utm_source: "$utm_source",
            utm_medium: "$utm_medium",
            collection: "$collection",
            product_id: "$product_id",
            option: "$option",
            order_id: "$order_id",
            cart_products: "$cart_products",
            cat_id: "$cat_id",
            collect_id: "$collect_id",
            viewing_product_id: "$viewing_product_id",
            recommendation_product_id: "$recommendation_product_id",
            recommendation_clicked_position: "$recommendation_clicked_position",
            price: "$price",
            currency: "$currency",
            is_paypal: "$is_paypal",
            key_search: "$key_search",
            recommendation_product_position: "$recommendation_product_position"
          },
          ids: { $push: "$_id" },
          count: { $sum: 1 }
      }
  },
  { $match: { count: { $gt: 1 } }}
// Removing duplicate rows
]).forEach(doc => {
  db.collection.deleteMany({
      _id: { $in: doc.ids.slice(1) } 
  });
});

// 4. Check data types consistency
db.SeminarCollection.aggregate([
  { $project: {
      price: 1, 
      priceType: { $type: "$price" },
      priceFormat: {
        $cond: [
          { $regexMatch: { input: { $toString: "$price" }, regex: /^[0-9]+\.[0-9]{2}$/ } }, 
          "decimal-dot",
          { $cond: [
            { $regexMatch: { input: { $toString: "$price" }, regex: /^[0-9]+,[0-9]{2}$/ } }, 
            "decimal-comma",
            "other"
          ] }
        ]}}
  },
  { $group: {
      _id: { type: "$priceType", format: "$priceFormat" }, 
      count: { $sum: 1 } // Đếm số lượng mỗi kiểu
    }
  },
  { $sort: { "_id.type": 1, "_id.format": 1 } }
])

// 5. Data Completeness
db.SeminarCollection.countDocuments();

// 6. Dữ liệu thu thập từ ngày nào đến ngày nào
db.SeminarCollection.aggregate([
  { $group: {
      _id: null,
      firstTimestamp: { $min: "$time_stamp" },
      lastTimestamp: { $max: "$time_stamp" },
      totalRecords: { $sum: 1 }
    }
  }
]);

// 7. Kiểm tra dữ liệu loại tiền tệ (currency)
db.SeminarCollection.aggregate([
  { $group: {
        _id: "$currency",
        count: { $sum: 1 }
      }
  },
  { $sort: {
        count: -1
      }}
])