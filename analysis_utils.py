from typing import Union

from TIMBER import Analyzer


def get_n_weighted(analyzer: Analyzer, data_flag: bool, weight_column: str = "genWeight") -> Union[int, float]:
    """Return the weighted number of events for the active TIMBER node."""
    if data_flag:
        return analyzer.DataFrame.Count().GetValue()
    return analyzer.DataFrame.Sum(weight_column).GetValue()


def get_n_events(analyzer: Analyzer) -> int:
    """Return the number of events for the active TIMBER node."""
    return analyzer.DataFrame.Count().GetValue()


def is_data(analyzer: Analyzer) -> bool:
    """Determine whether the active TIMBER dataset corresponds to data."""
    run_number = analyzer.DataFrame.Range(1).AsNumpy(["run"])
    return bool(run_number["run"][0] > 10000)