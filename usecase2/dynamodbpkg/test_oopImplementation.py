from dynamodbpkg import oop_implementation

TABLE_NAME = "MoviesR"

obj = oop_implementation.DynamoOps(False)
#obj.create_table(TABLE_NAME, 'title', 'S', 'year', 'N')
#print(obj.exists_table(TABLE_NAME))

# obj.put_batch(TABLE_NAME, 'moviesdata.json')

obj.scan_table(TABLE_NAME)