from pymongo import MongoClient
from bson.code import Code
import pprint

cars = [
	{"name":"Audi","price":"1","status":"a"},
	{"name":"Hummer","price":"2","status":"a"},
	{"name":"Skoda","price":"2","status":"a"},
	{"name":"Hummer","price":"3","status":"a"},
	{"name":"Volkswagen","price":"4","status":"a"},
	{"name":"Volvo","price":"5","status":"a"},
	{"name":"Audi","price":"6","status":"a"},
	{"name":"Hummer","price":"7","status":"a"},
	{"name":"Skoda","price":"8","status":"a"},
	{"name":"Hummer","price":"9","status":"a"},
	{"name":"Volkswagen","price":"10","status":"a"},
	{"name":"Volvo","price":"11","status":"a"},
	{"name":"Audi","price":"12","status":"b"},
	{"name":"Hummer","price":"13","status":"b"},
	{"name":"Skoda","price":"14","status":"b"},
	{"name":"Hummer","price":"15","status":"b"},
	{"name":"Volkswagen","price":"16","status":"b"},
	{"name":"Volvo","price":"17","status":"b"},
	{"name":"Audi","price":"18","status":"b"},
	{"name":"Hummer","price":"19","status":"b"},
	{"name":"Skoda","price":"20","status":"b"},
	{"name":"Hummer","price":"21","status":"b"},
	{"name":"Volkswagen","price":"22","status":"b"},
	{"name":"Volvo","price":"23","status":"b"},
	{"name":"Audi","price":"24","status":"a"},
	{"name":"Hummer","price":"25","status":"a"},
	{"name":"Skoda","price":"26","status":"a"},
	{"name":"Hummer","price":"27","status":"a"},
	{"name":"Volkswagen","price":"28","status":"a"},
	{"name":"Volvo","price":"29","status":"a"},
	{"name":"Audi","price":"30","status":"a"},
	{"name":"Hummer","price":"31","status":"a"},
	{"name":"Skoda","price":"32","status":"a"},
	{"name":"Hummer","price":"33","status":"a"},
	{"name":"Volkswagen","price":"34","status":"a"},
	{"name":"Volvo","price":"35","status":"a"},
	{"name":"Audi","price":"36","status":"b"},
	{"name":"Hummer","price":"37","status":"b"},
	{"name":"Skoda","price":"38","status":"b"},
	{"name":"Hummer","price":"39","status":"b"},
	{"name":"Volkswagen","price":"40","status":"b"},
	{"name":"Volvo","price":"41","status":"b"},
	{"name":"Audi","price":"42","status":"b"},
	{"name":"Hummer","price":"43","status":"b"},
	{"name":"Skoda","price":"44","status":"b"},
	{"name":"Hummer","price":"45","status":"b"},
	{"name":"Volkswagen","price":"46","status":"b"},
	{"name":"Volvo","price":"47","status":"b"}
]


client = MongoClient('mongodb://localhost:27017/')

with client:

	# accessing assignment 1 database from mongodb server
	db = client.assignment1

	#insertign cars into database
	print("\n cars inserting into db")
	db.carstest.insert_many(cars)

	print("\n printing cars in database")
	cars = db.carstest.find({})
	for car in cars:
		print(car)

	map = Code("""
		function(){ emit ( this.name, this.price ) }
		""")
	reduce = Code("""
		function(key,values){ return Array.sum(values) }""")

	# query:{ categories : ["Java"] },
	query = """	{
			query:{ status: "a" }
			out:"availabe_totals"		
		}
		"""

	result = db.books.map_reduce(map,reduce,query);
	print(result)
	for doc in result.find():
		pprint.pprint(doc)
	result.drop()

	print("deleting cars in database.")
	print(".")
	print(".")
	print(".")
	print(".")
	db.carstest.drop()
	print("All cars removed from databse successfully")
	cars = db.carstest.find({})
	for car in cars:
		print(car)