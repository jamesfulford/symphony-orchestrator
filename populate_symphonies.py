import json
import os
import typing

import pandas as pd
import requests
import quantstats

from lib import get_backtest_data, symphony_object, transpilers, traversers


def is_record_failed(record: dict) -> bool:
    return bool(record.get('failure_status', False))


def is_record_set_to_force(record: dict) -> bool:
    return bool(record.get('force_update', False))


def get_cache_path(symphony_id: str, filename: typing.Optional[str] = None):
    folderpath = f"outputs/symphonies/{symphony_id}"
    if not filename:
        return folderpath
    return f'{folderpath}/{filename}'


def read_symphony_cache_by_id(symphony_id: str) -> typing.Optional[dict]:
    try:
        return json.load(open(get_cache_path(symphony_id, "symphony.json")))
    except FileNotFoundError:
        return


def write_symphony_cache_by_id(symphony_id: str, symphony: dict):
    try:
        os.mkdir(get_cache_path(symphony_id))
    except FileExistsError:
        pass
    json.dump(symphony, open(
        get_cache_path(symphony_id, 'symphony.json'), 'w'), indent=4, sort_keys=True)


def download_symphony(symphony_id, force=False) -> typing.Optional[dict]:
    if not force and read_symphony_cache_by_id(symphony_id):
        return
    try:
        symphony = symphony_object.get_symphony(symphony_id)
    except requests.exceptions.HTTPError as e:
        return {
            'failure_status': f"Download fail: {e.response.status_code}",
            'failure_detail': f"{e.request.url}"
        }
    write_symphony_cache_by_id(symphony_id, symphony)


def main():
    symphonies = pd.read_csv('outputs/symphonies.csv', index_col="symphony_id")
    symphonies['symphony_id'] = symphonies.index
    symphonies['force_update'] = symphonies['force_update'].fillna("")
    symphonies['failure_status'] = symphonies['failure_status'].fillna("")
    # if forcing an update, forget past failures
    symphonies.loc[symphonies['force_update'] != "",
                   ['failure_status', 'failure_detail']] = ""

    records = symphonies.to_dict("records")

    # How TQQQ for the long term works (useful conditions)
    # https://www.reddit.com/user/derecknielsen/comments/yorwm0/educating_you_on_how_my_algo_tqqq_for_the_long/?context=3

    # Someone is tracking performance here:
    # https://docs.google.com/spreadsheets/d/1OnDiuLzfQ8yy6YNuOusdfUmlZvRXsQTPB-kH4O7eHK8/edit#gid=890537793

    # From Discover Page
    # 1. save copy to drafts
    # 2. make copy public
    # 3. paste ids here

    print("Updating community symphonies...")
    for record in [r for r in records if not is_record_failed(r)]:
        symphony_id = record['symphony_id']
        failure_updates = download_symphony(
            symphony_id, force=is_record_set_to_force(record))
        if failure_updates:
            record.update(failure_updates)
            continue

        symphony = read_symphony_cache_by_id(symphony_id)
        if not symphony:
            continue
        root_node = symphony_object.extract_root_node_from_symphony_response(
            symphony)
        record.update({
            "name": symphony["fields"]["name"]["stringValue"],
            "branches_count": len(traversers.collect_branches(root_node)),
            "unique_conditions_count": len(set([c['pretty_text'] for c in traversers.collect_conditions(root_node)])),
        })
    print("Updated community symphonies.")

    print("Reformatting downloaded symphonies to human.txt...")
    for record in [r for r in records if not is_record_failed(r)]:
        symphony_id = record['symphony_id']

        if not is_record_set_to_force(record) and os.path.exists(get_cache_path(symphony_id, "human.txt")):
            continue

        symphony = read_symphony_cache_by_id(symphony_id)
        if not symphony:
            continue

        print(symphony_id)
        print("  human format")
        with open(get_cache_path(symphony_id, 'human.txt'), 'w') as f:
            f.write(transpilers.HumanTextTranspiler.convert_to_string(
                symphony_object.extract_root_node_from_symphony_response(symphony)))
    print("Reformatted downloaded symphonies to human.txt.")

    print("Reformatting downloaded symphonies to vectorbt.py...")
    for record in [r for r in records if not is_record_failed(r)]:
        symphony_id = record['symphony_id']

        if not is_record_set_to_force(record) and os.path.exists(get_cache_path(symphony_id, "vectorbt.py")):
            continue

        symphony = read_symphony_cache_by_id(symphony_id)
        if not symphony:
            continue

        print(symphony_id)
        print("  vectorbt format")
        try:
            vectorbt_format = transpilers.VectorBTTranspiler.convert_to_string(
                symphony_object.extract_root_node_from_symphony_response(symphony))
            with open(get_cache_path(symphony_id, 'vectorbt.py'), 'w') as f:
                f.write(vectorbt_format)
        except Exception as e:
            record.update({
                'failure_status': f'Transpiler error: {e}',
                'failure_detail': f''
            })
            continue
    print("Reformatted downloaded symphonies to vectorbt.py.")

    print("Building allocation matrixes...")
    for record in [r for r in records if not is_record_failed(r)]:
        symphony_id = record['symphony_id']

        if not is_record_set_to_force(record) and os.path.exists(get_cache_path(symphony_id, "allocations.csv")) and os.path.exists(get_cache_path(symphony_id, "branch_tracker.csv")):
            continue

        symphony = read_symphony_cache_by_id(symphony_id)
        if not symphony:
            continue

        print(symphony_id)

        root_node = symphony_object.extract_root_node_from_symphony_response(
            symphony)
        tickers = traversers.collect_referenced_assets(root_node)
        closes = get_backtest_data.get_backtest_data(tickers)

        try:
            allocations, branch_tracker = transpilers.VectorBTTranspiler.execute(
                root_node, closes)
        except Exception as e:
            record.update({
                'failure_status': f'Backtest error {e}',
                'failure_detail': f''
            })
            continue
        allocations.to_csv(get_cache_path(symphony_id, "allocations.csv"))
        branch_tracker.to_csv(get_cache_path(
            symphony_id, "branch_tracker.csv"))
        record.update({
            "allocations_days": len(allocations),
            "branch_tracker_days": len(branch_tracker),
            "backtest_start": allocations.index.min().date().isoformat(),
            "backtest_end": allocations.index.max().date().isoformat(),
        })
    print("Built allocation matrixes.")

    print("Extracting returns...")
    for record in [r for r in records if not is_record_failed(r)]:
        symphony_id = record['symphony_id']

        if not is_record_set_to_force(record) and os.path.exists(get_cache_path(symphony_id, "returns.csv")):
            continue

        symphony = read_symphony_cache_by_id(symphony_id)
        if not symphony:
            continue

        print(symphony_id)

        allocations = pd.read_csv(
            get_cache_path(symphony_id, "allocations.csv"), parse_dates=True, index_col="Date")
        branch_tracker = pd.read_csv(
            get_cache_path(symphony_id, "branch_tracker.csv"), parse_dates=True, index_col="Date")

        root_node = symphony_object.extract_root_node_from_symphony_response(
            symphony)
        tickers = traversers.collect_referenced_assets(root_node)
        closes = get_backtest_data.get_backtest_data(tickers)

        try:
            returns = transpilers.VectorBTTranspiler.get_returns(
                closes, allocations, branch_tracker)
        except Exception as e:
            record.update({
                "failure_status": f"Failed to get returns: {e}",
                "failure_detail": f"",
            })
            continue
        returns.to_csv(get_cache_path(symphony_id, "returns.csv"))

        benchmark_ticker = record.get("benchmark_ticker", "SPY")
        closes = get_backtest_data.get_backtest_data(set([benchmark_ticker]))
        record.update({
            "Max Drawdown": quantstats.stats.max_drawdown(returns),
            "Sharpe": quantstats.stats.sharpe(returns),
            "Kelly": quantstats.stats.kelly_criterion(returns),

            "CAGR": quantstats.stats.cagr(returns),
            "Serenity": quantstats.stats.serenity_index(returns),
            # max drawdown is proportional to sqrt(time), so correct for that!
            "Adjusted Drawdown Risk": quantstats.stats.max_drawdown(returns) / ((len(allocations) / 252) ** 0.5),

            "rolling_kelly": quantstats.stats.kelly_criterion(returns.tail(126)),
            "rolling_beta": quantstats.stats.greeks(returns.tail(126), closes[benchmark_ticker].pct_change().dropna().tail(126))['beta'],
            "rolling_sharpe": quantstats.stats.sharpe(returns.tail(126)),

            "2weeks": typing.cast(float, (1+returns.tail(10)).prod()) - 1,
        })
    print("Extracted returns.")

    print("Writing reports...")
    for record in [r for r in records if not is_record_failed(r)]:
        symphony_id = record['symphony_id']

        if not is_record_set_to_force(record) and os.path.exists(get_cache_path(symphony_id, "VectorBT.html")):
            continue

        symphony = read_symphony_cache_by_id(symphony_id)
        if not symphony:
            continue

        print(symphony_id)

        returns = pd.read_csv(get_cache_path(
            symphony_id, "returns.csv"), parse_dates=True, index_col="Date")['group']
        benchmark_ticker = record.get("benchmark_ticker", "SPY")
        closes = get_backtest_data.get_backtest_data(set([benchmark_ticker]))

        quantstats.reports.html(
            returns,
            closes[benchmark_ticker].pct_change().dropna(),
            title=f"{symphony['fields']['name']['stringValue']} - VectorBT ({symphony_id})",
            output=get_cache_path(symphony_id, "VectorBT.html"), download_filename=get_cache_path(symphony_id, "VectorBT.html"))

        record.update({
            "report_url": f"file://{os.path.abspath(get_cache_path(symphony_id, 'VectorBT.html'))}",
        })
    print("Wrote reports.")

    print("Updating symphonies.csv...")
    df = pd.DataFrame(records)
    df['force_update'] = ""
    df = df.set_index("symphony_id")
    df.to_csv('outputs/symphonies.csv')
    print("Updated symphonies.csv.")
