__author__ = 'cpt'
"""
Utility functions for WRDS manipulations.

Generally, taking outside of wrdsapi the static methods.
"""

import re
import datetime
import os
import time


def rows_per_file_adjusted(dataset):
    """rows_per_file_adjusted(dataset)

    _rows_per_file chooses a number of rows to query in each
    _get_wrds_chunk request to ensure that the files produced
    do not approach the 1 GB server limit.  For most datasets,
    10^7 rows in a file is not a problem.  For optionm.opprcd,
    this number is dropped to 10^6.

    To date optionm.opprcd is the only dataset for which this has
    consistently been necessary.  This is subject to change
    with further use cases.

    return rows_per_file
    """
    rows_per_file = 10**7
    if dataset.replace('.', '_') == 'optionm_opprcd':
        rows_per_file = 10**6
    elif dataset.replace('.', '_') == 'optionm_optionmnames':
        rows_per_file = 10**6
    return rows_per_file


def get_loop_frequency(dataset, year):
    """get_loop_frequency(dataset, year) finds the best frequency at which
    to query the server for the given dataset so as to avoid producing
    problematically large files.

    return frequency
    """
    frequency = 'Y'
    if dataset.startswith('optionm'):
        if year < 2008:
            frequency = 'M'
        else:
            frequency = 'D'
    elif re.search('det_xepsus', dataset, flags=re.I):
        if year > 2005:
            frequency = 'M'
    elif re.search('det_xepsint', dataset, flags=re.I):
        if year > 2003:
            frequency = 'M'
    elif re.search('taq', dataset, flags=re.I):
        frequency = 'D'
    return frequency


def fix_weekdays(ymds, weekdays=1):
    """fix_weekdays(ymds, weekdays=1) takes a set of [year,month,date]
    tuples "ymds" and removes those which are not valid days,
    e.g. June 31, February 30.

    If weekdays is set to its default value of 1, it also removes
    Saturday and Sundays.

    return ymds
    """
    ymds2 = []
    for [y, m, d] in ymds:
        try:
            wday = datetime.date(y, m, d).weekday()
        except ValueError:
            wday = -1
        if weekdays == 1 and wday in range(5):
            # weekdays==1 --> only keep weekdays     #
            ymds2.append([y, m, d])
        elif weekdays == 0 and wday != -1:
            # weekdays==0 --> keey any valid day     #
            ymds2.append([y, m, d])
    return ymds2


def fix_input_name(dataset, year, month, day, rows=[]):
    """fix_input_name(dataset, year, month, day, rows=[])
    adjusts the user-supplied dataset name to use the same
    upper/lower case conventions as WRDS does.

    return [dataset, output_file]
    """
    [Y, M, D, R] = [year, month, day, rows]
    if year != 'all':
        ystr = '_'*(dataset[-1].isdigit()) + str(Y)
        mstr = '' + (M != 0)*('0'*(month < 10) + str(M))
        dstr = (D != 0)*('0'*(D < 10) + str(D))
        ymdstr = ystr + mstr + dstr + '.tsv'
        output_file = re.sub('\.', '_', dataset) + ymdstr
    else:
        output_file = re.sub('\.', '_', dataset) + '.tsv'

    if dataset.lower() == 'optionm.opprcd':
        dataset = dataset + str(year)

    elif dataset.lower() in ['taq.cq', 'taq.ct']:
        dataset = re.sub('cq', 'CQ', dataset)
        dataset = re.sub('ct', 'CT', dataset)
        ystr = '_' + str(Y)
        mstr = '' + (M != 0)*('0'*(M < 10) + str(M))
        dstr = '' + (D != 0)*('0'*(D < 10) + str(D))
        ymdstr = ystr + mstr + dstr
        dataset = dataset + ymdstr

    elif dataset.lower() in ['taq.mast', 'taq.div']:
        ymdstr = '_' + str(Y) + (M != 0)*('0'*(M < 10) + str(M))
        dataset = dataset + ymdstr

    elif dataset.lower() == 'taq.rgsh':
        ymdstr = str(100*Y + M)[2:]
        dataset = 'taq.RGSH' + ymdstr

    if R != []:
        rowstr = 'rows' + str(R[0]) + 'to' + str(R[1]) + '.tsv'
        output_file = re.sub('.tsv$', '', output_file) + rowstr

    return [dataset, output_file]


def wrds_datevar(filename):
    """wrds_datevar(filename)
    Different datasets in WRDS use different names for
    their date-variables.  wrds_datevar gives the right date
    variable for each dataset.  This may need periodic updating.
    Crowdsourcing is welcome.

    return date_var
    """
    if filename in ['tfn.s12', 'tfn.s34']:
        return 'fdate'
    if re.search('^crsp', filename):
        return 'date'
    if re.search('^comp', filename):
        return 'DATADATE'
    if re.search('^optionm\.opprcd', filename):
        return 'date'
    if re.search('^optionm', filename):
        return 'effect_date'
    if re.search('^ibes', filename):
        return 'anndats'
    return 'date'


def _wait_for_retrieve_completion(outfile, get_success, maxwait=1200):
    """_wait_for_retrieve_completion(outfile, get_success) checks the
    size of the downloaded file outfile multiple times and waits for
    two successive measurements giving the same file size.  Until
    this point, it infers that the download is still in progress.

    return local_size
    """
    if get_success == 0:
        return 0
    waited3 = 0
    [locmeasure1, locmeasure2, mtime2] = [0, 1, time.time()]
    write_file = '.' + outfile + '--writing'
    local_path = os.path.join(os.path.expanduser('~'), write_file)
    while ((waited3 < maxwait)
        and (locmeasure1 != locmeasure2 or (time.time() - mtime2) <= 10)):
        locmeasure1 = locmeasure2
        time.sleep(5)
        waited3 += 5
        local_stat = os.stat(local_path)
        locmeasure2 = local_stat.st_size
        mtime2 = local_stat.st_mtime

    if waited3 >= maxwait:
        print('get_wrds stopped waiting for SAS completion at step 3',
            locmeasure1, locmeasure2, mtime2)
        locmeasure1 = 0
    local_size = locmeasure1
    return local_size


def get_numlines(path2file):
    """get_numlines(path2file) reads a textfile located at
    path2file and returns the number of lines found.

    return numlines
    """
    with open(path2file, 'rb') as fd:
        fsize = os.stat(fd.name).st_size
        numlines = 0
        first_line = fd.readline().split('\t')
        while fd.tell() < fsize:
            fline = fd.readline()
            numlines += 1
    return numlines
