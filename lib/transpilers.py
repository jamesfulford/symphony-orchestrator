import abc
import typing

import pandas as pd
import pandas_ta
import vectorbt as vbt

from . import human, vectorbt, traversers


class Transpiler():
    @abc.abstractstaticmethod
    def convert_to_string(cls, root_node: dict) -> str:
        raise NotImplementedError()


class HumanTextTranspiler():
    @staticmethod
    def convert_to_string(root_node: dict) -> str:
        return human.convert_to_pretty_format(root_node)


def precompute_indicator(close_series: pd.Series, indicator: str, window_days: int):
    close = close_series.dropna()
    if indicator == ":cumulative-return":
        # because comparisons will be to whole numbers
        return close.pct_change(window_days) * 100
    elif indicator == ":moving-average-price":
        return pandas_ta.sma(close, window_days)
    elif indicator == ":relative-strength-index":
        return pandas_ta.rsi(close, window_days)
    elif indicator == ":exponential-moving-average-price":
        return pandas_ta.ema(close, window_days)
    elif indicator == ":current-price":
        return close_series
    elif indicator == ":standard-deviation-price":
        return pandas_ta.stdev(close, window_days)
    elif indicator == ":standard-deviation-return":
        return pandas_ta.stdev(close.pct_change() * 100, window_days)
    elif indicator == ":max-drawdown":
        # this seems pretty close
        maxes = close.rolling(window_days, min_periods=1).max()
        downdraws = (close/maxes) - 1.0
        return downdraws.rolling(window_days, min_periods=1).min() * -100
    elif indicator == ":moving-average-return":
        return close.pct_change().rolling(window_days).mean() * 100
    else:
        raise NotImplementedError(
            "Have not implemented indicator " + indicator)


class VectorBTTranspiler():
    @staticmethod
    def convert_to_string(root_node: dict) -> str:
        return vectorbt.convert_to_vectorbt(root_node)

    @staticmethod
    def execute(root_node: dict, closes: pd.DataFrame) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        code = VectorBTTranspiler.convert_to_string(root_node)
        locs = {}
        exec(code, {
            "pd": pd,
            "precompute_indicator": precompute_indicator,
        }, locs)
        build_allocations_matrix = locs['build_allocations_matrix']

        allocations, branch_tracker = build_allocations_matrix(closes)

        allocateable_tickers = traversers.collect_allocateable_assets(
            root_node)

        # remove tickers that were never intended for allocation
        for reference_only_ticker in [c for c in allocations.columns if c not in allocateable_tickers]:
            del allocations[reference_only_ticker]

        allocations_possible_start = closes[list(
            allocateable_tickers)].dropna().index.min().date()
        allocations = allocations[allocations.index.date >=
                                  allocations_possible_start]

        # aligning
        backtest_start = allocations.dropna().index.min().date()
        allocations = allocations[allocations.index.date >=
                                  backtest_start]
        branch_tracker = branch_tracker[branch_tracker.index.date >=
                                        backtest_start]

        return allocations, branch_tracker

    @staticmethod
    def extract_branches_with_incorrect_allocations(allocations, branch_tracker):
        branches_by_failed_allocation_days = branch_tracker[(
            allocations.sum(axis=1) - 1).abs() > 0.0001].sum(axis=0)
        return branches_by_failed_allocation_days[
            branches_by_failed_allocation_days != 0].index.values

    @staticmethod
    def get_returns(closes, allocations, branch_tracker) -> pd.Series:
        backtest_start = allocations.dropna().index.min().date()

        assert not len(VectorBTTranspiler.extract_branches_with_incorrect_allocations(
            allocations, branch_tracker)), "found incomplete allocations (!= 100%)"

        # VectorBT
        closes_aligned = closes[closes.index.date >=
                                backtest_start].reindex_like(allocations)
        portfolio = vbt.Portfolio.from_orders(
            close=closes_aligned,
            size=allocations,
            size_type="targetpercent",
            group_by=True,
            cash_sharing=True,
            call_seq="auto",
            freq='D',
            fees=0.0005,
        )
        returns = portfolio.asset_returns()
        # for some reason, the first entry is -inf, breaks some stats
        returns = returns.drop(index=backtest_start)
        return returns


def main():
    from . import symphony_object, get_backtest_data

    symphony_id = "KvA0KYc57MQSyykdWcFs"
    symphony = symphony_object.get_symphony(symphony_id)
    root_node = symphony_object.extract_root_node_from_symphony_response(
        symphony)

    print(HumanTextTranspiler.convert_to_string(root_node))
    print(VectorBTTranspiler.convert_to_string(root_node))

    tickers = traversers.collect_referenced_assets(root_node)

    closes = get_backtest_data.get_backtest_data(tickers)

    #
    # Execute logic
    #
    allocations, branch_tracker = VectorBTTranspiler.execute(
        root_node, closes)

    backtest_start = allocations.dropna().index.min().date()

    allocations_aligned = allocations[allocations.index.date >= backtest_start]
    branch_tracker_aligned = branch_tracker[branch_tracker.index.date >= backtest_start]

    assert len(allocations_aligned) == len(branch_tracker_aligned)

    print(allocations_aligned[(
        allocations_aligned.sum(axis=1) - 1).abs() > 0.0001])
    branches_by_failed_allocation_days = branch_tracker_aligned[(
        allocations_aligned.sum(axis=1) - 1).abs() > 0.0001].sum(axis=0)
    branches_with_failed_allocation_days = branches_by_failed_allocation_days[
        branches_by_failed_allocation_days != 0].index.values

    for branch_id in branches_with_failed_allocation_days:
        print(f"  -> id={branch_id}")
        print(allocations_aligned[branch_tracker_aligned[branch_id] == 1])
