from dynamodbpkg import oop_implementation

TABLE_NAME = "MoviesR"

obj = oop_implementation.DynamoOps()
print(obj.exists_table(TABLE_NAME))
# obj.delete_table(TABLE_NAME)

# obj.create_table(TABLE_NAME, 'title', 'S', 'year', 'N')

obj.put_batch(TABLE_NAME, "moviesdata.json")
itemdict = {
        "year": 2013,
        "title": "Rush",
        "info": {
            "directors": ["Ron Howard"],
            "release_date": "2013-09-02T00:00:00Z",
            "rating": 8,
            "genres": [
                "Action",
                "Biography",
                "Drama",
                "Sport"
            ],
            "image_url": "http://ia.media-imdb.com/images/M/MV5BMTQyMDE0MTY0OV5BMl5BanBnXkFtZTcwMjI2OTI0OQ@@._V1_SX400_.jpg",
            "plot": "A re-creation of the merciless 1970s rivalry between Formula One rivals James Hunt and Niki Lauda.",
            "rank": 2,
            "running_time_secs": 7380,
            "actors": [
                "Daniel Bruhl",
                "Chris Hemsworth",
                "Olivia Wilde"
            ]
        }
    }
obj.add_item(TABLE_NAME, itemdict )