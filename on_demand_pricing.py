#body = requests.get("https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv")

import csv

rec_by_key = {}

prices = {}
with open("index.csv") as fd:
    for i in range(5):
        fd.readline()

    d = csv.DictReader(fd)
    for rec in d:
        if rec["TermType"] != "OnDemand":
            continue
        if rec['Location'] != 'US East (N. Virginia)':
            continue
        if rec['Operating System'] != 'Linux':
            continue
        if rec['Tenancy'] != 'Shared':
            continue
        region='us-east-1'
        key = (region, rec['Instance Type'])
        if key in prices:
            prev = rec_by_key[key]
            for k in prev.keys():
                if prev[k] != rec[k]:
                    print ("prev[{}] = {}, cur[{}] = {}".format(k, prev[k], k, rec[k]))
        assert key not in prices, "{} has dup value: {}".format(key, rec)
        prices[key] = rec['PricePerUnit']
        rec_by_key[key] = rec

with open("prices_by_type.py", "wt") as fd:
    fd.write(repr(prices))
