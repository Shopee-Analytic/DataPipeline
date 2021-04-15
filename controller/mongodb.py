import pymongo
import json

with open("controller/accounts.json") as f:
    data = json.load(f)    
    admin = data['admin']

class ShopeeCrawlerDB:
    try:
        # Check if server is available
        assert pymongo.MongoClient("mongodb+srv://{}:{}@phase1.b2b5a.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE".format(admin["username"], admin["password"])), "cant connect to server"
    except AssertionError as msg:
        print(msg)
    else:
        client = pymongo.MongoClient("mongodb+srv://{}:{}@phase1.b2b5a.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE".format(admin["username"], admin["password"]))

        mydb = client['ShopeeCrawler']

        products = mydb['product']

    def insert_one_product(self, product_data):
        try:
            assert type(product_data) == dict, "Data is on wrong format, it should be a dictionary."
            # Check if product existed -> overwrite <> Handle duplicated data
            return self.products.update_one({"_id": product_data["_id"]}, {"$set": product_data}, upsert=True)
        except AssertionError as msg:
            print(msg)
            return None

    def insert_many_products(self, product_data):
        try:
            assert type(product_data) == list, "Data is on wrong format, it should be a list."
            for product in product_data:
                self.insert_one_product(product)
            print(f"Insert {len(product_data)} data to database!")
        except AssertionError as msg:
            print(msg)
            return None

    def find_all(self):
        return self.products.find({}, {"_id": 1})
    
    def drop_product(self):
        return self.products.drop()