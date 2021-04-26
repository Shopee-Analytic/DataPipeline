import pymongo
import json
from datetime import datetime
import concurrent.futures
import logging
with open("controller/accounts.json") as f:
    data = json.load(f)    
    admin = data['mongo']['admin']


# Version 1 - Data in 1 collection
# class ShopeeCrawlerDB:
#     client = pymongo.MongoClient("mongodb+srv://{}:{}@cluster0.b2b5a.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE".format(admin["username"], admin["password"]))
    
#     mydb = client['ShopeeCrawler']
#     products = mydb['product']

#     def insert_one_product(self, product_data):
#         return self.products.update_one({"_id": product_data["_id"]}, {"$set": product_data}, upsert=True).upserted_id # Check if product existed -> overwrite <> Handle duplicated data

#     def insert_many_products(self, product_data):
#         inserted_products = []
#         for product in product_data:
#             inserted_id = self.insert_one_product(product)
#             if inserted_id is not None:
#                 inserted_products.append(inserted_id)
#         return inserted_products
        
#     def find_all(self):
#         return self.products.find({}, {"_id": 1})
    

# Version "star" db
class ShopeeCrawlerDB:
    client = pymongo.MongoClient(admin['server_link'])
    
    mydb = client['ShopeeCrawler']

    FactProduct = mydb['FactProduct']
    Shop = mydb["Shop"]
    Rating = mydb["Rating"]
    Price = mydb["Price"]
    Feedback = mydb["Feedback"]
    Quantity = mydb["Quantity"]
    Time = mydb["Time"]
    
    # def insert_many_products(self, products):
    #     inserted_products = []
    #     for product in products:
    #         # print(product)
    #         inserted_id = self.insert_one_product(product)
    #         # if inserted_id is not None:
                
    #         inserted_products.append(inserted_id)
    #     return inserted_products

    def insert_many_products(self, products):
        futures = []
        inserted_products = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            try:
                for product in products:
                    futures.append(executor.submit(self.insert_one_product, product))
                for future in concurrent.futures.as_completed(futures):
                    inserted_products.append(future.result())
            except Exception as e:
                logging.error(e)
                executor.shutdown()
        return inserted_products

    def insert_one_product(self, product):
        data = {
            "_id": product["_id"],
            "product_name": product['product_name'],
            "category_id": product["category_id"],
            "image": product["image"],
            "product_link": product["product_link"],
            "feedback_id": self.insert_feedback(product),
            "shop_id": self.insert_shop(product),
            "rating_id": self.insert_rating(product),
            "price_id": self.insert_price(product),
            "quantity_id": self.insert_quantity(product),
            "time_id": self.insert_time(product)
        }
        return self.FactProduct.update_one(
            {"_id": product["_id"]},
            {"$set": data},
            upsert=True
        ).upserted_id

    def insert_shop(self, product):
        self.Shop.update_one(
            {"_id": product["shop_id"]},
            {"$set":{
                "_id": product["shop_id"],
                "shop_location": product["shop_location"],
                "shopee_verified": product['shopee_verified']
                }
            },
            upsert=True
        )
        return product["shop_id"]

    def insert_rating(self, product):
        data = {
            "rating_star": product["rating_star"],
            "rating_count": product["rating_count"]
        }
        try:
            return self.Rating.update_one(
                {"_id": self.find_one(product)["rating_id"]},    
                {"$set":data},
                upsert=True
                ).upserted_id
        except (TypeError, AttributeError):
            return self.Rating.insert_one(data).inserted_id
    def insert_price(self, product):
        data = {
            "price": product['price'],
            'currency': product['currency'],
            "discount": product["discount"]
        }
        try:
            return self.Price.update_one(
                {"_id": self.find_one(product)["price_id"]},
                {"$set": data},
                upsert=True
            ).upserted_id
        except (TypeError, AttributeError):
            return self.Price.insert_one(data).inserted_id

    def insert_feedback(self, product):
        data = {"feedback_count": product["feedback_count"]}
        try:
            return self.Feedback.update_one(
                {"_id": self.find_one(product)["feedback_id"]},
                {"$set": data},
                upsert=True
            ).upserted_id
        except (TypeError, AttributeError):
            return self.Feedback.insert_one(data).inserted_id

    def insert_quantity(self, product):
        data = {
            "sold": product["sold"],
            "stock": product['stock']
        }
        try:
            return self.Quantity.update_one(
                {"_id": self.find_one(product)["quantity_id"]},
                {"$set": data},
                upsert=True
            ).upserted_id
        except (TypeError, AttributeError):
            return self.Quantity.insert_one(data).inserted_id

    def insert_time(self, product):
        data = {
            "fetched_timestamp": product["fetched_timestamp"]
        }
        try:
            return self.Time.update_one(
                {"_id": self.find_one(product)["time_id"]},
                {"$set":{data}},
            upsert=True
            ).upserted_id
        except (TypeError, AttributeError):
            return self.Time.insert_one(data).inserted_id

    def find_one(self, product):
        return self.FactProduct.find_one({"_id": product["_id"]})