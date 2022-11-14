import json
import os
from lib import symphony_object, get_backtest_data, transpilers, traversers


def main():
    symphony_dirs = symphony_object.get_cached_symphony_ids()
    bad_backtest = {"F8PAOiqGFQLblW8zwooJ",
                    "RJPNmSmfFkgGv74sfAFx", "WthyE0LdbGloxn5NP7m3"}

    if not os.path.exists('outputs/branches'):
        os.mkdir('outputs/branches')

    for symphony_id in symphony_dirs:
        if symphony_id in bad_backtest:
            continue
        print(symphony_id)

        symphony = json.load(
            open(f'outputs/symphonies/{symphony_id}/symphony.json'))
        root_node = symphony_object.extract_root_node_from_symphony_response(
            symphony)
        tickers = traversers.collect_referenced_assets(root_node)

        closes = get_backtest_data.get_backtest_data(tickers)

        try:
            allocations, branch_tracker = transpilers.VectorBTTranspiler.execute(
                root_node, closes)
        except Exception as e:
            print(f"  skipping {e}")
            continue

        backtest_start = allocations.dropna().index.min().date()

        allocations_aligned = allocations[allocations.index.date >=
                                          backtest_start]
        branch_tracker_aligned = branch_tracker[branch_tracker.index.date >= backtest_start]

        # Make sure they are useful
        branches_by_failed_allocation_days = branch_tracker_aligned[(
            allocations_aligned.sum(axis=1) - 1).abs() > 0.0001].sum(axis=0)
        branches_with_failed_allocation_days = branches_by_failed_allocation_days[
            branches_by_failed_allocation_days != 0].index.values

        if len(branches_with_failed_allocation_days):
            print(f"  {len(branches_with_failed_allocation_days)}")

        # Investigate branches
        branches_by_path = traversers.collect_branches(root_node)
        branches_by_leaf_node_id = {
            key.split("/")[-1]: value for key, value in branches_by_path.items()}

        for branch_id in branch_tracker.columns:
            if not branch_tracker[branch_id].sum():
                continue
            node = traversers.find_node_by_id(root_node, branch_id)
            possible_allocations = traversers.collect_allocateable_assets(node)
            condition = branches_by_leaf_node_id[branch_id]
            print("  ", branch_id, branch_tracker[branch_id].sum(
            ))

            json.dump({
                "origin": symphony_id,
                "branch_id": branch_id,
                "condition": condition,
                "possible_allocations": list(possible_allocations),
                "node": node,
                "backtest_start": backtest_start.isoformat(),
            }, open(f'outputs/branches/{branch_id}.json', 'w'), indent=4, sort_keys=True)

            # TODO: treat subbranch as an algo, evaluate
