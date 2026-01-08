// Import the provided JSON file into collection 'products'

   //(Executed using mongoimport command in terminal)//
   
  // mongoimport --db fleximart --collection products --file products_catalog.json --jsonArray



// // Find all products in "Electronics" category with price less than 50000
// Return only: name, price, stock //

db.products.find(
  {
    category: "Electronics",
    price: { $lt: 50000 }
  },
  {
    _id: 0,
    name: 1,
    price: 1,
    stock: 1
  }
);


// // Find all products that have average rating >= 4.0 // // 
// Use aggregation to calculate average from reviews array //
 
 [
  {
    $unwind: "$reviews"
  },
  {
    $group: {
      _id: "$name",
      avg_rating: {
        $avg: "$reviews.rating"
      }
    }
  },
  {
    $match: {
      avg_rating: {
        $gte: 4
      }
    }
  }
]


// // Add a new review to product "ELEC001" // //
// Review: {user: "U999", rating: 4, comment: "Good value", date: ISODate()} // 
  
    {
  product_id: "ELEC001"
}
{
  reviews: {
    $concatArrays: [
      "$reviews",
      [
        {
          user_id: "U999",
          username: null,
          rating: 4,
          comment: "Good value",
          date: "Tue Dec 30 2025 - 05:32:08 GMT"
        }
      ]
    ]
  }
}



// Calculate average price by category
// Return: category, avg_price, product_count
// Sort by avg_price descending

db.products.aggregate([
  {
    $group: {
      _id: "$category",
      avg_price: { $avg: "$price" },
      product_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      category: "$_id",
      avg_price: 1,
      product_count: 1
    }
  },
  {
    $sort: { avg_price: -1 }
  }
])



