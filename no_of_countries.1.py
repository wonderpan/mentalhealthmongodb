# This query calculates the percentage of people per country willing to disclose mental health concerns in an interview.

import pymongo
from pymongo import MongoClient

client = MongoClient('172.31.85.91', 27017)
db = client["csci112proj"]
mental = db["mental_surv"]

first_test = mental.aggregate(
    [
        {
            "$group": { 
                "_id" : "$Country", 
                "totalCount" : { "$sum" : 1 },
                "countNo" : {
                    "$sum" : {
                        "$cond": [{"$eq" : ["$mental_health_interview", "No"]}, 1, 0]
                    }
                }
            }
        },
        {
            "$project" : {
                "_id" : 1,
                "percent_no": {
                    "$round": [
                        {
                            "$multiply": [
                                { "$divide": ["$countNo", "$totalCount"] },
                                100
                            ]
                        },
                        2
            ]
        }
        }},
        {
            "$sort" : {
                "percent_no" : -1
            }
        }
    ]
    )
    
for doc in first_test:
    print(doc)
