import pymongo
from pymongo import MongoClient
import re

client = MongoClient('172.31.85.91', 27017)
db = client["csci112proj"]
mental = db["mental_surv"]

patterns = {
    "Male": re.compile(r'^(m(ale)?|man|msle|mal|mail|maile|make|malr|male-ish|something kinda male\?)$', re.IGNORECASE),
    "Female": re.compile(r'^(f(emale)?|femake|femail|female \(cis\)|female \(trans\)|cis-?female\/femme|woman)$', re.IGNORECASE),
    "Non-binary": re.compile(r'^(non[-_ ]?binary|enby|androgyne|agender|genderqueer|queer(\/she\/they)?|fluid|nah|neuter)$', re.IGNORECASE)
}

print("Backing up and normalizing Gender...")

count_updated = 0

for doc in mental.find():
    gender_raw = doc.get("gender_raw")
    gender_val = doc.get("Gender", "").strip()

    if gender_raw:
        continue

    normalized = "Other"
    for label, pattern in patterns.items():
        if pattern.fullmatch(gender_val):
            normalized = label
            break

    result = mental.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "gender_raw": gender_val,
            "Gender": normalized
        }}
    )
    if result.modified_count > 0:
        count_updated += 1

print(f"{count_updated} documents updated and normalized.")
