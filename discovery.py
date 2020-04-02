# -*- coding: utf-8 -*-

import argparse
import errno
import json
import os
import re
import requests
import sys
import time
from functools import partial


from flask import Flask
app = Flask(__name__)

def flink_cluster_overview(jm_url):
    r = requests.get(jm_url+'/overview')
    if r.status_code != 200:
        return {}
    try:
        decoded = r.json()
    except  Exception as e:
        print("except job",jm_url)
    return decoded


def flink_jobmanager_prometheus_addr(jm_url):
    addr = None
    port = None

    r = requests.get(jm_url+'/jobmanager/config')
    if r.status_code != 200:
        return ''
    dic = {}
    for obj in r.json():
        dic[obj['key']] = obj['value']
    addr = dic['jobmanager.rpc.address']

    r = requests.get(jm_url + '/jobmanager/log', stream=True)
    if r.status_code != 200:
        return ''
    for line in r.iter_lines(decode_unicode=True):
        if "Started PrometheusReporter HTTP server on port" in line:
            m = re.search('on port (\d+)', line)
            if m:
                port = m.group(1)
                break

    cond1 = addr is not None
    cond2 = port is not None
    if cond1 and cond2:
        return addr+':'+port
    else:
        return ''


def flink_taskmanager_prometheus_addr(tm_id, jm_url, version):
    addr = None
    port = None

    r = requests.get(jm_url+'/taskmanagers/'+tm_id+'/log', stream=True)
    if r.status_code != 200:
        return ''

    for line in r.iter_lines(decode_unicode=True):
        if "hostname/address" in line:
            if version.startswith("1.4"):
                m = re.search("address '([0-9A-Za-z-_]+)' \([\d.]+\)", line)
                if m:
                    hostname = m.group(1)
                    addr = hostname
            else:
                m = re.search("TaskManager: ([0-9A-Za-z-.]+)", line)
                if m:
                    hostname = m.group(1)[:-1]
                    addr = hostname

        if "Started PrometheusReporter HTTP server on port" in line:
            m = re.search('on port (\d+)', line)
            if m:
                port = m.group(1)

        cond1 = addr is not None
        cond2 = port is not None
        if cond1 and cond2:
            return addr+':'+port

    return ''


def yarn_application_info(app_id, rm_addr):
    r = requests.get(rm_addr + '/ws/v1/cluster/apps/' + app_id)
    if r.status_code != 200:
        return {}

    decoded = r.json()
    return decoded['app'] if 'app' in decoded else {}


def taskmanager_ids(jm_url):
    r = requests.get(jm_url + '/taskmanagers')
    if r.status_code != 200:
        return []

    decoded = r.json()
    if 'taskmanagers' not in decoded:
        return []

    return [tm['id'] for tm in decoded['taskmanagers']]


def prometheus_addresses(app_id, rm_addr):
    prom_addrs = []
    while True:
        app_info = yarn_application_info(app_id, rm_addr)
        if 'trackingUrl' not in app_info:
            time.sleep(1)
            continue

        jm_url = app_info['trackingUrl']
        jm_url = jm_url[:-1] if jm_url.endswith('/') else jm_url

        overview = flink_cluster_overview(jm_url)
        if 'flink-version' not in overview:
            time.sleep(1)
            continue
        version = overview['flink-version']

        if 'taskmanagers' not in overview:
            time.sleep(1)
            continue
        taskmanagers = overview['taskmanagers']

        if app_info['runningContainers'] == 1:
            print('runningContainers({app_info[\"runningContainers\"]}) is 1' )
            time.sleep(1)
            continue

        if app_info['runningContainers'] != taskmanagers+1:
            print('runningContainers({app_info["runningContainers"]}) != jobmanager(1)+taskmanagers({taskmanagers})')
            time.sleep(1)
            continue

        tm_ids = taskmanager_ids(jm_url)
        prom_addrs = map(partial(flink_taskmanager_prometheus_addr, jm_url=jm_url, version=version), tm_ids)
        prom_addrs = list(filter(lambda x: len(x) > 0, prom_addrs))
        if len(tm_ids) != len(prom_addrs):
            print('Not all taskmanagers open prometheus endpoints. {len(tm_ids)} of {len(prom_addrs)} opened')
            time.sleep(1)
            #continue
            return None
        break

    while True:
        jm_prom_addr = flink_jobmanager_prometheus_addr(jm_url)
        if len(jm_prom_addr) == 0:
            time.sleep(1)
            continue
        prom_addrs.append(jm_prom_addr)
        break

    encoded = json.JSONEncoder().encode([{'targets': prom_addrs}])
    return encoded


def create_json_file(rm_addr, target_dir, app_id):
    target_string = prometheus_addresses(app_id, rm_addr)
    if target_dir is not None and target_string is not None:
        path = os.path.join(target_dir, app_id + ".json")
        with open(path, 'w') as f:
            print('{path} : {target_string}')
            f.write(target_string)
    else:
        print(target_string)


def delete_json_file(target_dir, app_id):
    if target_dir is not None:
        path = os.path.join(target_dir, app_id + ".json")
        print('{path} deleted')
        try:
            os.remove(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                # re-raise exception if a different error occurred
                raise


@app.route('/flink')
def index():
    rm_addr = "http://yarn.xxx.com"

    running_cur = {}
    added = set()
    removed = set()

    r = requests.get(rm_addr+'/ws/v1/cluster/apps')
    decoded = r.json()
    apps = decoded['apps']['app']

    all_data=[]

    for app in apps:
        if app['state'].lower() == 'running' and app['applicationType'] == 'Apache Flink':
            target_string = prometheus_addresses(app['id'], rm_addr)
            if target_string != None:
                for i in json.loads(target_string):
                    all_data.append(i)
    print("all is >>",all_data)
    return json.JSONEncoder().encode(all_data)


if __name__ == '__main__':
     app.run(host="0.0.0.0")

