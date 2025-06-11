import pymongo
from pymongo import MongoClient

client = MongoClient("172.31.85.91", 27017)
db = client["csci112proj"]
mental = db["mental_surv"]

pipeline = [
  { 
    "$match": { "wellness_program": "Yes" }
  },

  {
    "$group": {
      "_id": "$wellness_program",         
      "total_wellness_count": { "$sum": 1 },
      "consequence_count": {
        "$sum": {
          "$cond": [
            { "$eq": ["$obs_consequence", "Yes"] }, 
            1, 
            0
          ]
        }
      }
    }
  }
]


result = mental.aggregate(pipeline)

for doc in result:
    print(doc)