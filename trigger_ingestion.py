import argparse
import collections
import json
import os
import pprint
import sys
import time
import typing

import requests

import datetime

AIRFLOW_API_BASE_URL = 'http://host.docker.internal:8080/api/v1'
AIRFLOW_USER = 'airflow'
AIRFLOW_PASSWORD = 'airflow'


class ApiException(Exception):
    pass


def parse_args(args):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('--input-dir', required=True)
    parser_run.add_argument('--workspace-dir', required=True)
    parser_run.add_argument('--dag-id', default='graphite_ds_per_file')
    parser_run.add_argument('--run-id-prefix', default='')
    parser_run.add_argument('--limit', type=int, default='0')
    parser_run.add_argument('--patients', type=csv, default='')
    parser_run.set_defaults(func=run_subcmd)

    parser_analyze = subparsers.add_parser('analyze')
    parser_analyze.add_argument('--num-top-errors', type=int, default='10')
    parser_analyze.add_argument('validation_dir')
    parser_analyze.set_defaults(func=analyze_subcmd)

    parser_find_exampels = subparsers.add_parser('find-examples')
    parser_find_exampels.add_argument('--error-string', default='')
    parser_find_exampels.add_argument('--limit', type=int, default='1')
    parser_find_exampels.add_argument('--code')
    parser_find_exampels.add_argument('--code-display')
    parser_find_exampels.add_argument('run_dir')
    parser_find_exampels.set_defaults(func=find_examples_subcmd)

    parser_find_least_errors = subparsers.add_parser('find-least-errors')
    parser_find_least_errors.add_argument('--exclude-error-strings', default='', type=pipe_separated)
    parser_find_least_errors.add_argument('run_dir')
    parser_find_least_errors.set_defaults(func=find_least_errors_subcmd)

    parser_show_matches = subparsers.add_parser('show-matches')
    parser_show_matches.add_argument('run_dir')
    parser_show_matches.set_defaults(func=show_matched_unmatched_subcmd)

    parser_post_notifications = subparsers.add_parser('post-notifications')
    parser_post_notifications.add_argument('--tracking-service-base-url', default='http://localhost:8081')
    parser_post_notifications.add_argument('--client-id', required=True)
    parser_post_notifications.add_argument('run_dir')
    parser_post_notifications.set_defaults(func=post_validation_notifications_subcmd)

    parser_find_resource = subparsers.add_parser('find-resource')
    parser_find_resource.add_argument('--id', required=True)
    parser_find_resource.add_argument('run_dir')
    parser_find_resource.set_defaults(func=find_resource_by_id_subcmd)

    return parser.parse_args(args)


def pipe_separated(s: str) -> typing.List[str]:
    result = []
    if s:
        result = s.split("|")
    return result


def trigger_dag_api(dag_id, conf=None, dag_run_id=None):
    url = f"{AIRFLOW_API_BASE_URL}/dags/{dag_id}/dagRuns"
    payload = {}
    if dag_run_id:
        payload['dag_run_id'] = dag_run_id
    if conf:
        payload['conf'] = conf
    r = requests.post(url=url, json=payload, auth=(AIRFLOW_USER, AIRFLOW_PASSWORD))
    if r.status_code / 100 != 2:
        raise ApiException(f"Non-success status code from Airflow API {r.status_code}")
    return r.json()


def get_dag_run_api(dag_id, dag_run_id):
    url = f"{AIRFLOW_API_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"
    r = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASSWORD))
    if r.status_code / 100 != 2:
        raise ApiException(f"Non-success status code from Airflow API {r.status_code}")
    return r.json()


def trigger_dag(dag_id, file_name, workspace_dir, parent_run_id):
    conf = {'workspace_dir': workspace_dir, 'file_name': file_name, 'parent_run_id': parent_run_id}
    return trigger_dag_api(dag_id, conf=conf)


def make_dirs(workspace_dir, run_id):
    os.mkdir(os.path.join(workspace_dir, run_id))
    for d in ('standardized', 'assigned', 'validated', 'term_notifications'):
        os.mkdir(os.path.join(workspace_dir, run_id, d))


def analyze_file(validation_file: str, params=None) -> typing.Dict:
    with open(validation_file, 'r') as f:
        data = json.load(f)
        total_resources = 0
        profile_assigned = 0
        all_errors = collections.defaultdict(int)
        gold = []
        for r in data:
            total_resources += 1
            if 'profile' in r:
                profile_assigned += 1
                issues = r['validations']['issue']
                errors = [i for i in issues if i['severity'] == 'error']
                if len(errors) == 0:
                    gold.append(r['id'])
                for e in errors:
                    k = e['diagnostics']
                    all_errors[k] += 1

        # top_errors = {}
        # for e in sorted(all_errors, key=lambda k: all_errors[k], reverse=True)[:10]:
        #     top_errors[e] = all_errors[e]

        return {
            'total': total_resources,
            'profile_assigned': profile_assigned,
            'gold': gold,
            'top_errors': all_errors
        }


def summarize(analyses: typing.List[typing.Dict], num_top_errors: int):
    total_resources = sum([a['total'] for a in analyses])
    profile_assigned = sum([a['profile_assigned'] for a in analyses])
    all_gold = []
    for a in analyses:
        all_gold.extend(a['gold'])
    total_gold = len(all_gold)
    if total_gold > 0:
        print("GOLD FOUND!!!")
    else:
        print("No gold found :-(")

    top_errors = collections.defaultdict(int)
    for a in analyses:
        errs = a['top_errors']
        for e, num in errs.items():
            top_errors[e] += num

    sorted_top_n = sorted(top_errors, key=lambda k: top_errors[k], reverse=True)[:num_top_errors]

    print(
        f"Total resources = {total_resources}\nResources with CEM profiles assigned = {profile_assigned}\nGold resources = {total_gold}")
    if total_gold > 0:
        print(f"Some resource ids of gold resources: {','.join(all_gold[:10])} ")

    print(f"Top {num_top_errors} errors")
    print("=============")
    print("\n" + "\n".join([f"{top_errors[k]} {k}" for k in sorted_top_n]))


def main(args):
    args = parse_args(args)
    args.func(args)


def analyze_subcmd(args):
    analyze(args.validation_dir, args.num_top_errors)


def run_subcmd(args):
    if args.patients:
        patients = []
        for p in args.patients:
            pat = p
            if not p.endswith('.csv'):
                pat = p + '.csv'
            patients.append(pat)
        sorted_files = sorted(patients)
    else:
        sorted_files = sorted(os.listdir(args.input_dir))
    if args.limit > 0:
        sorted_files = sorted_files[:args.limit]
    run_id = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    if args.run_id_prefix:
        run_id = args.run_id_prefix + '-' + run_id
    make_dirs(args.workspace_dir, run_id)
    dag_runs = {}
    for f in sorted_files:
        file_name = os.path.join(args.input_dir, f)
        dag = trigger_dag(args.dag_id, file_name, args.workspace_dir, run_id)
        dag_runs[dag['dag_run_id']] = dag

    start = time.time()
    wait_for_completion(args.dag_id, dag_runs)
    end = time.time()
    time_taken = end - start
    print(f"INFO: Completed all dag runs in {time_taken:.2f} seconds!!!")

    validation_dir = os.path.join(args.workspace_dir, run_id, 'validated')
    analyze(validation_dir, 10)

def wait_for_completion(dag_id, dag_runs):
    while not all_dag_runs_completed(dag_runs):
        new_dag_runs = {}
        for run_id, dag in dag_runs.items():
            try:
                if dag['state'] not in ('success', 'failed'):
                    d = get_dag_run_api(dag_id, run_id)
                    new_dag_runs[run_id] = d
                else:
                    new_dag_runs[run_id] = dag
            except Exception as e:
                new_dag_runs[run_id] = dag
                print(f"WARN: Failed to get DAG run for id {run_id}, error = {e}")
        dag_runs = new_dag_runs
        print_stats(dag_runs)
        time.sleep(2)

def print_stats(dag_runs: typing.Dict):
    stats = collections.defaultdict(int)
    for v in dag_runs.values():
        stats[v['state']] += 1

    print()
    print("Progress")
    print("========")
    for k, v in stats.items():
        print(f"{k} {v}")
    print()


def all_dag_runs_completed(dag_runs) -> bool:
    return all([v['state'] in ('success', 'failed') for k, v in dag_runs.items()])


def analyze(validation_dir, num_top_errors):
    analyses = []
    for f in os.listdir(validation_dir):
        file_name = str(os.path.join(validation_dir, f))
        a = analyze_file(file_name)
        analyses.append(a)

    summarize(analyses, num_top_errors)


def find_examples_subcmd(args):
    examples = find_examples(args.run_dir, args.error_string, args.code, args.code_display, args.limit)
    if examples:
        print(json.dumps(examples[:args.limit], indent=4))
    else:
        print("No examples found :-(")


def find_examples(run_dir, error_str: str, code: str, code_display: str, num_examples: int):
    validation_files = os.listdir(os.path.join(run_dir, 'validated'))
    examples_found = 0
    examples = []
    done = False
    for vf in validation_files:
        validation_file = os.path.join(run_dir, 'validated', vf)
        with open(validation_file, 'r') as f:
            data = json.load(f)
            for r in data:
                if 'profile' in r:
                    issues = r['validations']['issue']
                    if any([i for i in issues if i['severity'] == 'error' and error_str in i['diagnostics']]):
                        joined_resource = _join_resource_with_validation(run_dir, {vf: [r]})[0]
                        if _does_code_or_display_match(joined_resource, code, code_display):
                            examples.append(joined_resource)
                            examples_found += 1
                            if examples_found == num_examples:
                                done = True
                                break
        if done:
            break

    return examples


def _does_code_or_display_match(resource: typing.Dict, code: str, code_display: str) -> bool:
    if code:
        return 'code' in resource and 'coding' in resource['code'] and resource['code']['coding'][1]['code'] == code
    elif code_display:
        return 'code' in resource and 'coding' in resource['code'] and code_display in resource['code']['coding'][1]['display']
    return True


def _join_resource_with_validation(run_dir, validation_resources: typing.Dict) -> typing.List:
    example_resources = []
    for fname, example_rs in validation_resources.items():
        ids = [r['id'] for r in example_rs]
        assigned_file = os.path.join(run_dir, 'assigned', fname)
        with open(assigned_file, 'r') as f:
            data = json.load(f)
            for e in data['entry']:
                if e['resource']['id'] in ids:
                    resource = e['resource']
                    v = [v for v in example_rs if v['id'] == resource['id']][0]
                    resource['validations'] = v['validations']
                    example_resources.append(resource)
    return example_resources


def find_resource_by_id_subcmd(args):
    examples = find_resource_by_id(args.run_dir, args.id)
    if examples:
        print(json.dumps(examples, indent=4))
    else:
        print("No resource found")


def find_resource_by_id(run_dir, resource_id):
    validation_files = os.listdir(os.path.join(run_dir, 'validated'))
    result = None
    for vf in validation_files:
        validation_file = os.path.join(run_dir, 'validated', vf)
        with open(validation_file, 'r') as f:
            data = json.load(f)
            for r in data:
                if r['id'] == resource_id:
                    result = {vf: [r]}
                    break

    return _join_resource_with_validation(run_dir, result)


def find_least_errors_subcmd(args):
    examples = find_resources_with_least_errors(args.run_dir, args.exclude_error_strings)
    if examples:
        print(json.dumps(examples, indent=4))
    else:
        print("No examples found :-(")


def find_resources_with_least_errors(run_dir, exclude_error_strings: typing.List[str]) -> typing.List:
    validation_files = os.listdir(os.path.join(run_dir, 'validated'))
    done = False
    min_errors_found = 99999
    the_vf = None
    the_r = None
    for vf in validation_files:
        validation_file = os.path.join(run_dir, 'validated', vf)
        with open(validation_file, 'r') as f:
            data = json.load(f)
            for r in data:
                exclude_resource = False
                if 'profile' in r:
                    issues = r['validations']['issue']
                    errors = [i for i in issues if i['severity'] == 'error']
                    if len(errors) == 0:
                        # We don't want gold
                        continue
                    for excluded in exclude_error_strings:
                        if any([i for i in errors if excluded in i['diagnostics']]):
                            exclude_resource = True
                            break
                    if exclude_resource:
                        continue
                    if len(errors) < min_errors_found:
                        min_errors_found = len(errors)
                        the_r = r
                        the_vf = vf
                    if min_errors_found == 1:
                        done = True
                        break
        if done:
            break
    return _join_resource_with_validation(run_dir, {the_vf: [the_r]})


def show_matched_unmatched_subcmd(args):
    result = find_matched_unmatched(args.run_dir)
    print("Resources that matched CEM profiles:")
    print('%-10s ' % 'code', '%-16s' % 'system', '%-40s' % 'display')
    #print('%-10s ' % '-' * len('code'), '%-16s' % '-' * len('system'), '%-40s' % '-' * len('display'))
    for k in result['matched']:
        system, code, display = k
        print('%-10s ' % code, '%-16s' % system, '%-40s' % display)

    print()
    print()
    print("Resources that did not match CEM profiles:")
    print('%-10s ' % 'code', '%-16s' % 'system', '%-40s' % 'display')
    for k in result['unmatched']:
        system, code, display = k
        print('%-10s ' % code, '%-16s' % system, '%-40s' % display)


def find_matched_unmatched(run_dir) -> typing.Dict:
    result = {'matched': collections.defaultdict(int), 'unmatched': collections.defaultdict(int)}
    assigned_dir = os.path.join(run_dir, 'assigned')
    assigned_files = os.listdir(assigned_dir)

    for af in assigned_files:
        assigned_file = os.path.join(assigned_dir, af)
        with open(assigned_file, 'r') as f:
            data = json.load(f)
            for e in data['entry']:
                resource = e['resource']
                if resource['resourceType'] == 'Observation' and 'code' in resource and 'coding' in resource['code']:
                    coding = resource['code']['coding']
                    if len(coding) > 1:
                        translated = coding[1]
                        key = (translated['system'], translated['code'], translated['display'])
                        matched = 'meta' in resource and 'profile' in resource['meta']
                        if matched:
                            result['matched'][key] += 1
                        else:
                            result['unmatched'][key] += 1

    return result


def csv(s):
    result = []
    if s:
        result = s.split(',')
    return result


def post_validation_notifications_subcmd(args):
    post_validation_notifications(args.run_dir, args.tracking_service_base_url, args.client_id)


def post_validation_notifications(run_dir, base_url, client_id):
    track_req = _create_request(client_id, base_url)
    severity_map = {
        'information': 'INFO',
        'warning': 'WARNING',
        'error': 'ERROR'
    }
    validation_files = os.listdir(os.path.join(run_dir, 'validated'))
    for vf in validation_files:
        track_seg = _create_segment(track_req['id'], client_id, base_url)
        track_source = _create_source(track_seg['id'], client_id, base_url)
        _post_terminology_notifications(run_dir, base_url, vf, track_seg['id'], client_id)
        validation_file = os.path.join(run_dir, 'validated', vf)
        with open(validation_file, 'r') as f:
            data = json.load(f)
            for r in data:
                dest = _create_destination(r, track_source['id'], client_id, base_url)
                notifications = []
                for i in r['validations']['issue']:
                    sev = severity_map[i['severity']]
                    notification = {
                        "phase": "VALIDATION",
                        "systemId": client_id,
                        "severity": sev,
                        "type": "VALIDATION",
                        "message": i['diagnostics'],
                    }
                    if 'location' in i:
                        notification['location'] = json.dumps(i['location'])
                    notifications.append(notification)
                _post_resource_notifications(base_url, dest['id'], "DESTINATION", notifications)


def _create_destination(resource, ref_id, client_id, base_url):
    data = {"processType": "DESTINATION", "processAction": "CREATE", "clientId": client_id, "resourceId": resource['id'], "refId": ref_id}
    r = requests.post(base_url + '/track/create', json=data)
    if r.status_code not in (200, 201):
        raise ApiException(f"Non-success status code from tracking-service: {r.status_code}, body is {r.text}")
    return r.json()


def _create_request(client_id, base_url):
    data = {"processType": "REQUEST", "processAction": "CREATE", "clientId": client_id, "reqType": "API"}
    r = requests.post(base_url + '/track/create', json=data)
    if r.status_code not in (200, 201):
        raise ApiException(f"Non-success status code from tracking-service: {r.status_code}, body is {r.text}")
    return r.json()


def _create_segment(req_id, client_id, base_url):
    data = {"processType": "SEGMENT", "processAction": "CREATE", "clientId": client_id, "refId": req_id}
    r = requests.post(base_url + '/track/create', json=data)
    if r.status_code not in (200, 201):
        raise ApiException(f"Non-success status code from tracking-service: {r.status_code}, body is {r.text}")
    return r.json()


def _create_source(seg_id, client_id, base_url):
    data = {"processType": "SOURCE", "processAction": "CREATE", "clientId": client_id, "refId": seg_id}
    r = requests.post(base_url + '/track/create', json=data)
    if r.status_code not in (200, 201):
        raise ApiException(f"Non-success status code from tracking-service: {r.status_code}, body is {r.text}")
    return r.json()


def _post_resource_notifications(base_url, ref_id, ref_type, notifications):
    notification_req = {
        "referenceType": ref_type,
        "referenceId": ref_id,
        "notifications": notifications
    }
    r = requests.post(base_url + '/track/notifications', json=notification_req)
    if r.status_code not in (200, 201):
        raise ApiException(f"Non-success status code from tracking-service: {r.status_code}, body is {r.text}")
    return r.json()


def _post_terminology_notifications(run_dir, base_url, file_name, seg_id, client_id):
    term_file = os.path.join(run_dir, 'term_notifications', file_name)
    with open(term_file, 'r') as tf:
        terms = json.load(tf)
        unmapped = [term for term in terms if term['mappingStatus'] == 'UNMAPPED']
        if not unmapped:
            return
        notifications = []
        for term in unmapped:
            notification = {
                "phase": "STANDARDIZATION",
                "systemId": client_id,
                "severity": "ERROR",
                "type": "TERMINOLOGY",
                "message": f"Could not map term {term['originalTerm']}",
                "originalCode": term['originalCode'],
                "originalTerm": term["originalTerm"],
                "originalCodeSystem": term["originalCodeSystem"],
                "mapReference": term["mapReference"],
                "category": term["category"],
            }
            notifications.append(notification)
        _post_resource_notifications(base_url, seg_id, "SEGMENT", notifications)

if __name__ == '__main__':
    main(sys.argv[1:])
