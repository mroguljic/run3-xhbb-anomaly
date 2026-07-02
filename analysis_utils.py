from typing import Union

from TIMBER import Analyzer

import re


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

def get_pdf_errtype(lhaID):
    """
    Helper script to get PDF information for given MC file(s) to pass to the C++ TIMBER module.
    Given an LHA ID, determines the correct PDF set and error type for PDF weight calculations.
    """
    with open('/cvmfs/sft.cern.ch/lcg/external/lhapdfsets/current/pdfsets.index', 'r', encoding='utf-8') as f:
        pdfsets = f.read()
    pdfset = re.findall(f'^.*{lhaID}.*$', pdfsets, re.M)        # Get the correct PDF, given the LHA ID
    assert(len(pdfset) == 1); pdfset = pdfset[0].split(' ')[1]  # Get only the name of the PDF set
    lhapdf_path = f'/cvmfs/sft.cern.ch/lcg/external/lhapdfsets/current/{pdfset}'
    lhapdf_info = f'{lhapdf_path}/{pdfset}.info'
    with open(lhapdf_info, 'r', encoding='utf-8') as f:
        info = f.read()
    errtype = re.findall('^.*ErrorType.*$', info, re.M)
    assert(len(errtype) == 1); errtype = errtype[0].split(' ')[-1]
    return errtype