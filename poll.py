import boto.ec2
import boto.ec2.cloudwatch
import datetime
from tinydb import TinyDB, Query
import collections

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
    
    db = TinyDB(db_filename, indent=2)
    db.insert(snapshot)
    # prune old snapshots > 5 days
    last_timestamp_to_keep = datetime.datetime.now() - datetime.timedelta(0, 60*60*24*5)
    db.remove(Query().timestamp < last_timestamp_to_keep.isoformat())

    snapshots = db.all()
    snapshots.sort(key=lambda x: x['timestamp'])
    return snapshots

def check_spend(snapshot_history, max_spend):
    last_snapshot = snapshots[-1]
    current_spend = calc_total_spend(last_snapshot)
    if max_spend < current_spend:
        report("exceeded-max-spend", "Current hourly spend ${}/hour > max spend ${}/hour".format(current_spend, max_spend))

def check_cpu(snapshot_history, host_configs):
    too_low = collections.defaultdict(lambda: set())
    last_snapshot = snapshots[-1]
    for inst in last_snapshot['instances']:
        name = inst['name']
        last_cpu = inst['last_cpu']
        if last_cpu is None:
            last_cpu_avg = 0
        else:
            last_cpu_avg = last_cpu['cpu_util_percent']
        
        host_config = find_matching_host(host_configs, name)
        if host_config.min_cpu_avg > last_cpu_avg:
            too_low[host_config.name].add( (name, last_cpu_avg) )
    
    for name, examples in too_low.items():
        report(name+"-cpu-too-low", "The following hosts reported low cpu usage: {}".format(examples))

import attr
import re
import sys

def find_matching_host(configs, name):
    for c in configs:
        if re.match(c.pattern, name) is not None:
            return c
    raise Exception("No match: "+name)

HostConfig = attr.make_class("HostConfig", ["name", "pattern", "min_cpu_avg"])
host_configs = [
    HostConfig("master", "master", 0),
    HostConfig("aws03", "aws03", 0),
    HostConfig("star-cluster-node", "node[0-9]+", 90),
    HostConfig("default", ".*", 1e10)
   ]

reported_errors = []
def report(error_key, message):
    global had_error
    reported_errors.append( (error_key, message) )

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check status of EC2 nodes")
    parser.add_argument("db", help="path to file to use to store snapshot of data collected from EC2")
    parser.add_argument("--max_spend", type=float, help="alert if $ per/hour is exceeded", default=0.10)
    args = parser.parse_args()

    snapshots = update(args.db)
    check_spend(snapshots, args.max_spend)
    check_cpu(snapshots, host_configs)
    for error_key, message in reported_errors:
        print("{}: {}".format(error_key, message))
    if len(reported_errors) > 0:
        sys.exit(1)
    print("okay")

