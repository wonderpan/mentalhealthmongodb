# This query seeks to analyze the percentage of people in the survey who are not willing to open up to supervisors and co-workers given the existence of the following: 
# 1. Mental health benefits
# 2. Wellness benefits
# 3. Access to resources for seeking help


import pymongo
from pymongo import MongoClient

client = MongoClient('172.31.85.91', 27017)
db = client["csci112proj"]
mental = db["mental_surv"]

pipeline = [
    {
        "$facet": {
            "closedness_if_benefits": [
                { "$match": { "benefits": "Yes" } },
                { "$group": {
                    "_id": None,
                    "total_benefit_count": { "$sum": 1 },
                    "benefit_coworkers": {
                        "$sum": { "$cond": [{ "$eq": ["$coworkers", "No"] }, 1, 0] }
                    },
                    "benefit_supervisor": {
                        "$sum": { "$cond": [{ "$eq": ["$supervisor", "No"] }, 1, 0] }
                    }
                }},
                { "$project": {
                    "_id" : 0,
                    "total_benefit_count": 1,
                    "pct_closed_coworkers": {
                        "$round": [
                            { "$multiply": [
                                { "$divide": ["$benefit_coworkers", "$total_benefit_count"] },
                                100
                            ]},
                            2
                        ]
                    },
                    "pct_closed_supervisor": {
                        "$round": [
                            { "$multiply": [
                                { "$divide": ["$benefit_supervisor", "$total_benefit_count"] },
                                100
                            ]},
                            2
                        ]
                    }
                }}
            ],
            "closedness_if_wellness": [
                { "$match": { "wellness_program": "Yes" } },
                { "$group": {
                    "_id": None,
                    "total_wellness_count": { "$sum": 1 },
                    "wellness_coworkers": {
                        "$sum": { "$cond": [{ "$eq": ["$coworkers", "No"] }, 1, 0] }
                    },
                    "wellness_supervisor": {
                        "$sum": { "$cond": [{ "$eq": ["$supervisor", "No"] }, 1, 0] }
                    }
                }},
                { "$project": {
                    "_id" : 0,
                    "total_wellness_count": 1,
                    "pct_closed_coworkers": {
                        "$round": [
                            { "$multiply": [
                                { "$divide": ["$wellness_coworkers", "$total_wellness_count"] },
                                100
                            ]},
                            2
                        ]
                    },
                    "pct_closed_supervisor": {
                        "$round": [
                            { "$multiply": [
                                { "$divide": ["$wellness_supervisor", "$total_wellness_count"] },
                                100
                            ]},
                            2
                        ]
                    }
                }}
            ],
            "closedness_if_seek_help": [
                { "$match": { "seek_help": "Yes" } },
                { "$group": {
                    "_id": None,
                    "total_help_count": { "$sum": 1 },
                    "help_coworkers": {
                        "$sum": { "$cond": [{ "$eq": ["$coworkers", "No"] }, 1, 0] }
                    },
                    "help_supervisor": {
                        "$sum": { "$cond": [{ "$eq": ["$supervisor", "No"] }, 1, 0] }
                    }
                }},
                { "$project": {
                    "_id" : 0,
                    "total_help_count": 1,
                    "pct_closed_coworkers": {
                        "$round": [
                            { "$multiply": [
                                { "$divide": ["$help_coworkers", "$total_help_count"] },
                                100
                            ]},
                            2
                        ]
                    },
                    "pct__supervisor": {
                        "$round": [
                            { "$multiply": [
                                { "$divide": ["$help_supervisor", "$total_help_count"] },
                                100
                            ]},
                            2
                        ]
                    }
                }}
            ]
        }
    }
]

result = list(mental.aggregate(pipeline))

for facet_name, docs in result[0].items():
    print(f"\n--- {facet_name} ---")
    for doc in docs:
        print(doc)
