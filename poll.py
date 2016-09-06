import boto.ec2
import boto.ec2.cloudwatch
import datetime
from tinydb import TinyDB, Query

class Prices:
    def __init__(self,c, filename):
        self.c=c
        with open(filename, "rt") as fd:
            self.prices = eval(fd.read())
        self.spot_cache = {}

    def get_price(self, is_spot, instance_type, zone):
        assert not is_spot
        if is_spot:
            key = (zone, instance_type)
            if key in self.spot_cache:
                return self.spot_cache[key]
            now = datetime.datetime.now()
            h = self.c.get_spot_price_history(now.isoformat(), now.isoformat(), instance_type, availability_zone=zone)
            price = max([x.price for x in h])
            self.spot_cache[key] = price
            return price
        else:
            region = zone[:-1]
            price = float(self.prices[ (region, instance_type) ])
            assert price > 0, "region={}, instance_type={} has zero price".format(region, instance_type)
            return price

def get_cpu_utilization(cw, instance_id):
    now = datetime.datetime.now()
    stats = cw.get_metric_statistics(60, now - datetime.timedelta(0, 60*30), now, "CPUUtilization", "AWS/EC2", ["Average"], dimensions=dict(InstanceId=instance_id), unit=None)
    if len(stats) < 1:
        return None
    last = stats[-1]
    return dict(timestamp = last['Timestamp'].isoformat(), cpu_util_percent=last['Average'])

def get_snapshot(prices, regions):
    cw = boto.ec2.cloudwatch.CloudWatchConnection()
    now = datetime.datetime.now()
    result = []
    for region in regions:
        c = boto.ec2.connect_to_region(region)
        assert c is not None
        instances = c.get_only_instances()
        for instance in instances:
            if instance.state in ['terminated', 'stopped']:
                continue

            is_spot = instance.spot_instance_request_id is not None
            last_cpu = get_cpu_utilization(cw, instance.id)
            i = dict(name=instance.tags['Name'], region=region, id=instance.id, type=instance.instance_type, price=prices.get_price(is_spot, instance.instance_type, instance.placement), last_cpu=last_cpu)
            result.append(i)
    return dict(timestamp=now.isoformat(), instances=result)

def calc_total_spend(snapshot):
    a = 0
    for rec in snapshot['instances']:
        a += rec['price']
    return a

def update(db_filename):
    prices=Prices(boto.ec2.connect_to_region("us-east-1"), "prices_by_type.py")
    snapshot = get_snapshot(prices, ["us-east-1"])
    
    db = TinyDB(db_filename)
    db.insert(snapshot)
    # prune old snapshots > 5 days
    last_timestamp_to_keep = datetime.datetime.now() - datetime.timedelta(0, 60*60*24*5)
    db.remove(Query().timestamp < last_timestamp_to_keep.isoformat())

    snapshots = db.all()
    snapshots.sort(key=lambda x: x['timestamp'])
    last_snapshot = snapshots[-1]

    print(calc_total_spend(last_snapshot))

if __name__ == "__main__":
    update("db.json")
