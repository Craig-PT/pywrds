__author__ = 'cpt'
"""Initial extraction of sas queries from wrdsapi.

Idea to make into a general class to generate queries to be passed through
wrdsapi.
"""

from . import utility as wrds_util
import os
import re


def wrds_sas_script(download_path, dataset, year, month=0, day=0, rows=[]):
    """Generates a .sas file.
     To be executed on the WRDS server to produce the desired dataset.

     e.g. sample request.

        DATA new_data;
            SET crsp.dsf (where = ((year(date) between 2008 and 2008) and
            (month(date) between 2 and 2) and (day(date) between 2 and 2)));
            IF (1<= _N_<= 10000000);

        proc export data = new_data
            outfile = "~/crsp_dsf20080202rows1to10000000.tsv"
            dbms = tab
            replace;
            putnames = yes;
        run;

    :param download_path: path for local sas script.
    :param dataset:
    :param year:
    :param month:
    :param day:
    :param rows:
    :return [sas_file, output_file, dataset]:
    """
    ystr = '' + ('_' + str(year)) * (year != 'all')
    mstr = '' + (month != 0)*('0'*(month < 10) + str(month))
    dstr = '' + (day != 0)*('0'*(day < 10) + str(day))
    ymdstr = ystr + mstr + dstr
    sas_file = 'wrds_export_' + re.sub('\.', '_', dataset)

    if rows:
        row_str = 'rows' + str(rows[0]) + 'to' + str(rows[1])
        sas_file += ymdstr + row_str
    else:
        sas_file += ymdstr
    sas_file += '.sas'

    [dataset, output_file] = \
        wrds_util.fix_input_name(dataset, year, month, day, rows)

    with open(os.path.join(download_path, sas_file), 'wb') as fd:
        fd.write('DATA new_data;\n')
        fd.write('\tSET ' + dataset)
        if year != 'all':
            where_query = ' (where = ('
            year_query = ('(year(' + wrds_util.wrds_datevar(dataset) + ')'
                + ' between ' + str(year) + ' and ' + str(year) + ')')
            where_query += year_query

            if month != 0:
                month_query = (' and (month(' + wrds_util.wrds_datevar(dataset)
                    + ') between ' + str(month) + ' and ' + str(month)+')')
                where_query += month_query

            if day != 0:
                day_query = (' and (day(' + wrds_util.wrds_datevar(dataset)
                    + ') between ' + str(day) + ' and ' + str(day) + ')')
                where_query += day_query

            where_query += '));\n'
            fd.write(where_query)
        else:
            fd.write(';\n')

        if rows:
            row_query = ('\tIF (' + str(rows[0]) + '<= _N_<= ' + str(rows[1]) +
                        '); \n')
            fd.write(row_query)

        fd.write('\n')
        fd.write('proc export data = new_data\n')
        fd.write(('\toutfile = "~/' + output_file + '" \n'
                  + '\tdbms = tab \n'
                  + '\treplace; \n'
                  + '\tputnames = yes; \n'
                  + 'run; \n'))
    return [sas_file, output_file, dataset]

