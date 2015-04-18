__author__ = 'cpt'
"""
Utility functions for WRDS manipulations.

Generally, taking outside of wrdsapi any static methods.
"""

import re
import datetime
import os
import time


def rows_per_file_adjusted(dataset):
    """ Chooses a number of rows to query to ensure that the files produced
    do not approach the 1 GB server limit on WRDS.

    For most datasets, 10^7 rows in a file is not a problem.  For
    optionm.opprcd, this number is dropped to 10^6.

    To date optionm.opprcd is the only dataset for which this has
    consistently been necessary.  This is subject to change with further use
    cases.

    :param dataset:
    :return rows_per_file:
    """
    rows_per_file = 10**7
    if dataset.replace('.', '_') == 'optionm_opprcd':
        rows_per_file = 10**6
    elif dataset.replace('.', '_') == 'optionm_optionmnames':
        rows_per_file = 10**6
    return rows_per_file


def get_loop_frequency(dataset, year):
    """Finds the best frequency at which to query the server for the given
    dataset so as to avoid producing problematically large files.

    :param dataset:
    :param year:
    :return frequency:
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
    """Takes a set of [year, month, date] tuples "ymds" and removes those which
    are not valid days, e.g. June 31, February 30.

    If weekdays is set to its default value of 1, it also removes
    Saturday and Sundays.

    :param ymds:
    :param weekdays:
    :return ymds:
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
    """Adjusts the user-supplied dataset name to use the same upper/lower
    case conventions as WRDS does.

    :param dataset:
    :param year:
    :param month:
    :param day:
    :param rows:
    :return [dataset, output_file]:
    """
    [Y, M, D, R] = [year, month, day, rows]
    if year != 'all':
        ystr = '_' * (dataset[-1].isdigit()) + str(Y)
        mstr = '' + (M != 0) * ('0' * (month < 10) + str(M))
        dstr = (D != 0)*('0'*(D < 10) + str(D))
        ymdstr = ystr + mstr + dstr + '.tsv'
        output_file = re.sub('\.', '_', dataset) + ymdstr
    else:
        output_file = re.sub('\.', '_', dataset) + '.tsv'

    if dataset.lower() == 'optionm.opprcd':
        dataset += str(year)

    elif dataset.lower() in ['taq.cq', 'taq.ct']:
        dataset = re.sub('cq', 'CQ', dataset)
        dataset = re.sub('ct', 'CT', dataset)
        ystr = '_' + str(Y)
        mstr = '' + (M != 0)*('0'*(M < 10) + str(M))
        dstr = '' + (D != 0)*('0'*(D < 10) + str(D))
        ymdstr = ystr + mstr + dstr
        dataset += ymdstr

    elif dataset.lower() in ['taq.mast', 'taq.div']:
        ymdstr = '_' + str(Y) + (M != 0)*('0'*(M < 10) + str(M))
        dataset += ymdstr

    elif dataset.lower() == 'taq.rgsh':
        ymdstr = str(100*Y + M)[2:]
        dataset = 'taq.RGSH' + ymdstr

    if R:
        rowstr = 'rows' + str(R[0]) + 'to' + str(R[1]) + '.tsv'
        output_file = re.sub('.tsv$', '', output_file) + rowstr

    return [dataset, output_file]


def wrds_datevar(filename):
    """Different datasets in WRDS use different names for their
    date-variables.  Function returns the right date variable for each
    dataset.

    This may need periodic updating. Crowdsourcing is welcome.

    :param filename:
    :return date_var:
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


def wait_for_retrieve_completion(outfile, get_success, max_wait=1200):
    """Checks size of downloaded outfile until two successive
    give the same result.
    Until this point, it infers that the download is still in progress.

    :param outfile:
    :param get_success:
    :param max_wait:
    :return: local_size
    """
    if get_success == 0:
        return 0

    [total_wait, local_size, local_size_delayed, mtime2] = \
        [0, 0, 1, time.time()]

    write_file = '.' + outfile + '--writing'
    local_path = os.path.join(os.path.expanduser('~'), write_file)

    while total_wait < max_wait and local_size != local_size_delayed:
        local_size = local_size_delayed
        time.sleep(5)
        total_wait += 5
        local_stat = os.stat(local_path)
        local_size_delayed = local_stat.st_size
        mtime2 = local_stat.st_mtime

    if total_wait >= max_wait:
        print('get_wrds stopped waiting for SAS completion at step 3, %s, %s, '
              '%s', (local_size, local_size_delayed, mtime2))
        local_size = 0

    return local_size


def get_numlines(path2file):
    """Returns number of lines for a text files located at path2file.

    :param path2file:
    :return numlines:
    """
    with open(path2file, 'rb') as fd:
        fsize = os.stat(fd.name).st_size
        numlines = 0
        first_line = fd.readline().split('\t')
        while fd.tell() < fsize:
            fline = fd.readline()
            numlines += 1
    return numlines


def get_numlines_from_log(outfile, dname):
    """Reads SAS log file created in get_wrds to find the number of
    lines which the wrds server says should be in a downloaded
    file "outfile".

    This number can then be checked against the number actually found in
    the file.

    :param outfile:
    :param dname:
    :return logfile_lines:
    """
    log_lines = -1
    sasfile = 'wrds_export_' + re.sub('\.tsv$', '.log', outfile)
    if not os.path.exists(os.path.join(dname, sasfile)):
        partial_fname = re.sub('[0-9]*rows.*', '', sasfile)
        sasfile2 = partial_fname+'_'+re.sub(partial_fname, '', sasfile)
        if os.path.exists(os.path.join(dname, sasfile2)):
            sasfile = sasfile2
        all_fname = re.sub('rows', '_allrows', sasfile)
        if os.path.exists(os.path.join(dname, all_fname)):
            sasfile = all_fname

    if os.path.exists(os.path.join(dname, sasfile)):
        with open(os.path.join(dname, sasfile)) as fd:
            fsize = os.stat(fd.name).st_size
            while fd.tell() < fsize:
                fline = fd.readline()
                if re.search('^[0-9]* records created in ', fline):
                    log_lines = re.split(' records created in ', fline)[0]
                    log_lines = int(float(log_lines))
                    break

                pattern0 = ('NOTE: The data set WORK\.NEW_DATA '
                    +'has [0-9]* observations')
                if re.search(pattern0, fline):
                    pattern01 = 'NOTE: The data set WORK\.NEW_DATA has '
                    pattern02 = ' observations'
                    split_log = re.split(pattern02, fline)[0]
                    log_lines = re.split(pattern01, split_log)[-1]
                    log_lines = int(float(log_lines))
                    break

                pattern1 = 'NOTE: [0-9]* records were written to the file'
                if re.search(pattern1, fline):
                    split_log = re.split('NOTE: ', fline)[-1]
                    log_lines = re.split('records', split_log)[0]
                    log_lines = int(float(log_lines))
                    break

                # The numbers given by the pattern below are often     #
                # one row lower than the numbers given by the above    #
                # patterns, the latter being the desired answer.       #
                # This code is kept as an option to re-implement       #
                # should their arise cases where none of the other     #
                # patterns are found.                                  #
                #pattern2 = 'NOTE: There were [0-9]* observations read'
                #if re.search(pattern2,fline):
                #	split_log = re.split(' observations read',fline)[0]
                #	log_lines = re.split('NOTE: There were ',split_log)[-1]
                #	log_lines = int(float(log_lines))
                #	break
    return log_lines


def _recombine_ready(fname, dname=None, suppress=0):
    """Checks files downloaded by get_wrds to see if the loop has
    completed successfully and the files are ready to be be recombined.

    If dname == None, the directory defaults to os.getcwd().

    :param fname:
    :param dname:
    :param suppress:
    :return isready (bool):
    """
    if not dname:
        dname = os.getcwd()
    isready = 1
    fname0 = re.sub('rows[0-9][0-9]*to[0-9][0-9]*\.tsv', '', fname)

    if os.path.exists(os.path.join(dname, fname + '.tsv')):
        isready = 0

    rows_per_file = rows_per_file_adjusted(fname0)
    flist0 = os.listdir(dname)
    flist0 = [x for x in flist0 if x.endswith('.tsv')]
    flist0 = [x for x in flist0 if re.search(fname0, x)]
    fdict = {x: x.split('rows')[-1] for x in flist0}
    fdict = {x: re.split('_?to_?',fdict[x])[0] for x in fdict}
    fdict = {x: float(fdict[x]) for x in fdict if fdict[x].isdigit()}
    flist = [[fdict[x], x] for x in fdict]

    if isready and flist == []:
        isready = 0
        if suppress == 0:
            print('recombine_ready: No such files found: ' + fname)

    numlist = [x[0] for x in sorted(flist)]
    missing_nums = [x for x in numlist if x != 1]
    missing_nums = [x for x in missing_nums if x-rows_per_file not in numlist]

    if isready and missing_nums != []:
        isready = 0
        if suppress == 0:
            print('recombine_ready: ' + fname
                + ' missing_nums ' + repr(missing_nums+numlist))

    end_nums = [re.sub('\.tsv$', '', x[1]) for x in flist]
    end_nums = [re.split('to', x)[-1] for x in end_nums]
    end_nums = [float(x) for x in end_nums]

    if isready and end_nums != [] and max(end_nums)%rows_per_file == 0:
        max_num = int(max(end_nums))
        flist2 = [x[1] for x in flist if x[1].endswith(repr(max_num)
                                                       + '.tsv')]
        if len(flist2) == 1:
            outfile = flist2[0]
            numlines = get_numlines(os.path.join(dname, outfile))
            log_numlines = get_numlines_from_log(outfile, dname)
            if numlines != log_numlines:
                isready = 0
                print('recombine_ready: '+outfile
                    +' numlines!=log_numlines: '
                    +repr([numlines, log_numlines]))
        else:
            isready = 0
            if suppress == 0:
                print('recombine_ready: ' + fname + ' appears incomplete: '
                      + repr(max(end_nums)))
    return isready


def recombine_files(fname, dname=None, suppress=0):
    """Reads the files downloaded by get_wrds and combines them
    back into the single file of interest.

    If dname==None, the directory defaults to os.getcwd().

    :param fname:
    :param dname:
    :param suppress:
    :return num_combined_files:
    """
    if not dname:
        dname = os.getcwd()
    combined_files = 0
    if not _recombine_ready(fname, dname, suppress):
        return combined_files

    fname0 = re.sub('rows[0-9][0-9]*to[0-9][0-9]*\.tsv', '', fname)
    rows_per_file = rows_per_file_adjusted(fname0)

    flist0 = [x for x in os.listdir(dname) if re.search(fname0, x)]
    flist0 = [x for x in flist0 if x.endswith('.tsv')]
    fdict = {x: x.split('rows')[-1] for x in flist0}
    fdict = {x: re.split('_?to_?',fdict[x])[0] for x in fdict}
    fdict = {x: float(fdict[x]) for x in fdict if fdict[x].isdigit()}
    flist = [[fdict[x], x] for x in fdict]

    flist = [x[1] for x in sorted(flist)]
    with open(os.path.join(dname, flist[-1]), 'rb') as fd:
        fsize = os.stat(fd.name).st_size
        nlines = 0
        while fd.tell() > fsize:
            fd.readline()
            nlines += 1

    if nlines >= rows_per_file:
        print([fname, flist[-1],
        'len(flines)=' + repr(nlines),
        'should_be=' + repr(rows_per_file)])
        return combined_files

    with open(os.path.join(dname, fname0+'.tsv'), 'wb') as fd:
        headers = []
        found_problem = 0
        for fname1 in flist:
            fd1 = open(os.path.join(dname, fname1), 'rb')
            fsize1 = os.stat(fd1.name).st_size
            headers1 = fd1.readline().strip('\r\n')
            if headers == []:
                headers = headers1
                fd.write(headers1 + '\n')
            if headers1 != headers:
                print('Problem with header matching:' + fname1)
                found_problem = 1
            if found_problem == 0:
                try:
                    while fd1.tell() < fsize1:
                        fd.write(fd1.readline().strip('\r\n') + '\n')
                    fd1.close()
                except KeyboardInterrupt:
                    fd1.close()
                    fd.close()
                    os.remove(fd.name)
                    raise KeyboardInterrupt
                combined_files += 1

    if found_problem == 0:
        for fname1 in flist:
            os.remove(os.path.join(dname, fname1))
    return combined_files



