__author__ = 'cpt'
import os
import time
import pandas as pd


class BaseQuery(object):
    """Container class for a sas query and its filename.
    """
    def __init__(self, query, query_name):
        self.query = query
        self.file_name = query_name
        self._local_path = ''
        # Set the output of the query to be a .tsv file and store the file
        # trunk.
        self.trunk = query_name.split('.')[0]
        self.out_filename = self.trunk + '.tsv'
        self.log_file = self.trunk + '.log'

    def _write2local(self, local_path):
        """Write query to local_path with self.file_name and store its location.
        """
        self.ensure_dir(local_path)
        with open(os.path.join(local_path, self.file_name), 'wb') as fd:
            fd.write(self.query)

        # Set local path, as removing file will be conditional on file being
        # written
        self._local_path = local_path
        return

    def _remove_from_local(self, file_path):
        """Remove the file from the local path.
        """
        try:
            os.remove(file_path)
        except IOError:
            print('There was an error opening the file.')
            return
        # self._local_path, self.file_name: os.remove()
        return True

    def _write_results2remote(self, session):
        """

        :param filename:
        :return:
        """
        table_name = 'out.crsp_rets'
        write_remote_query = """
        proc export data = {0}
            outfile = "~/{1}"
            dbms = tab
            replace;
            putnames = yes;
        run;
        """.format(table_name, self.out_filename)

        query = BaseQuery(write_remote_query, self.trunk + '_temp.sas')

        return session.run_query(query)

    def write_results2local(self, session):
        """

        :param session:
        :return:
        """
        tic = time.time()
        status = self._write_results2remote(session)

        outfile = self.out_filename

        # Download to local.
        session.get_remote_file(outfile)

        check_file = os.path.join(session.download_path, outfile)
        if os.path.exists(check_file):
            return [1, time.time()-tic]
        return [0, time.time()-tic]

    def return_dataframe(self, session):
        """

        :param session:
        :return:
        """
        # TODO: 1. Check query has been written to the local directory
        #   a. if not, run write_results2local()

        # 2. Return as pandas DataFrame
        return pd.DataFrame.from_csv(self.out_filename, sep='\t',
                                     index_col=False)

    @staticmethod
    def get_nobs(session, log_filename, delimiter=None):
        """When creating a table with sql, the n obs is in this format:
            "NOTE: Table OUT.CRSP_RETS created, with 70536 rows and 6 columns."
        So, for time being just take the first number following delimiter
        'created, with'

        :param session (WrdsSession):
        :param log_filename:
        :param delimiter: String after which the first number is returned.
        :return:
        """
        # Default delimiter for PROC SQL.
        if not delimiter:
            delimiter = 'created, with'

        # Recover the first digits after the delimiter.
        cmd = "perl -nle 'print $1 if /{0}.*?(\d+)/' {1}".format(delimiter,
                                                                 log_filename)

        [stdin, stdout, stderr] = session.ssh.exec_command(cmd)
        n_obs_raw = (stdout.readlines())
        return n_obs_raw[0].strip()

    @staticmethod
    def ensure_dir(path):
        """Creates directory if doesn't exist. Directories require trailing
        slash.
         Handles 2 cases:
            1. path = /usr/dir1/
            2. path = /usr/dir1/file.txt
        Will throw error if path doesn't contain trailing slash and is not a
        file, e.g. 'path = /usr/dir1'.
        """
        # Check to see if the path passed is a file path or directory path.
        # Directories are assumed to have trailing slashes.
        head, tail = os.path.split(path)

        # If the tail non-empty, assumed to be a file name - Checks that it
        # contains at least one period.
        if tail:
            assert '.' in tail, "Filename assumed to contain period, neither " \
                                "a valid directory or filename. Convention, " \
                                "is directories must contain trailing slashes."

        d = os.path.dirname(path)
        if not os.path.exists(d):
            os.makedirs(d)


def eg_query_crsp():
    query = """
    /* Year Range */
    %LET FIRST = 1965;
    %LET LAST = 1967;
    %LET crsp_rets = a.PERMNO, a.DATE, b.SICCD, b.EXCHCD, b.SHRCD, a.RET;
        proc sql;
        create table out.crsp_rets as
            select &crsp_rets
            from crsp.msf as a
            inner join crsp.mseexchdates as b
            on a.PERMNO = b.PERMNO and a.DATE between b.NAMEDT and b.NAMEENDT
            and year(date) between &FIRST and &LAST
            where SICCD not between 6000 and 6999 and EXCHCD in (1,2,3) and SHRCD in (10,11)
            order by a.PERMNO, a.DATE;
            ;
        quit;
        """
    return BaseQuery(query, 'test_crsp.sas')


def eg_nobs_crsp():
    query = """
    proc sql noprint;
        select count(*) into : nobs
        from out.crsp_rets;
    quit;
    %put 'Obs in data set:' &nobs;
    """
    alt_query = """
    proc sql noprint;
        select nlobs into : nobs
        from dictionary.tables
        where libname='out'
        and memname='crsp_rets';
    quit;
    %put 'Obs in data set:' &nobs;
    """
    return BaseQuery(query, 'n_crsp.sas')

