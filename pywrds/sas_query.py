__author__ = 'cpt'
"""Initial extraction of sas queries from wrdsapi.

Idea to make into a general class to generate queries to be passed through
wrdsapi.
"""

from . import utility as wrds_util
import os
import re


def wrds_sas_script(download_path, dataset, year, month=0, day=0, rows=[]):
    """Generates a .sas file which is executed on the WRDS server to produce
    the desired dataset.

    :param download_path:
    :param dataset:
    :param year:
    :param month:
    :param day:
    :param rows:
    :return [sas_file, output_file, dataset]:
    """
    [Y, M, D, R] = [year, month, day, rows]
    ystr = '' + ('_' + str(Y))*(Y != 'all')
    mstr = '' + (M != 0)*('0'*(M < 10) + str(M))
    dstr = '' + (D != 0)*('0'*(D < 10) + str(D))
    ymdstr = ystr + mstr + dstr
    sas_file = 'wrds_export_' + re.sub('\.', '_', dataset)

    if R != []:
        rowstr = 'rows' + str(R[0]) + 'to' + str(R[1])
        sas_file = sas_file + ymdstr + rowstr
    else:
        sas_file = sas_file + ymdstr
    sas_file = sas_file + '.sas'

    [dataset, output_file] = wrds_util.fix_input_name(dataset, Y, M, D, R)
    with open(os.path.join(download_path, sas_file), 'wb') as fd:
        fd.write('DATA new_data;\n')
        fd.write('\tSET ' + dataset)
        if Y != 'all':
            where_query = ' (where = ('
            year_query = ('(year(' + wrds_util.wrds_datevar(dataset) + ')'
                + ' between ' + str(Y) + ' and ' + str(Y) + ')')
            where_query = where_query + year_query

            if M != 0:
                month_query = (' and (month(' + wrds_util.wrds_datevar(dataset)
                    +') between '+str(M)+' and '+str(M)+')')
                where_query = where_query + month_query

            if D != 0:
                day_query = (' and (day(' + wrds_util.wrds_datevar(dataset)
                    +') between '+str(D)+' and '+str(D)+')')
                where_query = where_query+day_query

            where_query = where_query+'));\n'
            fd.write(where_query)
        else:
            fd.write(';\n')

        if R != []:
            rowquery = ('\tIF ('+str(R[0])+'<= _N_<= '+str(R[1])+');\n')
            fd.write(rowquery)

        fd.write('\n')
        fd.write('proc export data = new_data\n')
        fd.write(('\toutfile = "~/'+output_file+'" \n'
                    +'\tdbms = tab \n'
                    +'\treplace; \n'
                    +'\tputnames = yes; \n'
                    +'run; \n'))
    return [sas_file, output_file, dataset]

