__author__ = 'cpt'

import os
import time
import pandas as pd


class BaseQuery(object):
    """Container class for a sas query and its filename.
    """
    def __init__(self, wrds_session, query, query_name, out_table_name,
                 write_output_table2log=False):
        # TODO: Check user vbl of user defined class without importing it.
        # TODO: Split into library, table_name, query_name.
        # TODO: Add possibility for multiple output tables / way to access them
        # Use simple list: ['table_name_1', 'table_name_2']
        self.session = wrds_session
        self.query = query
        self.file_name = query_name
        self._local_path = ''

        # Location of final result table of query.
        self.out_table_name = out_table_name

        # Set the output of the query to be a .tsv file and store the file
        # trunk.
        self.trunk = query_name.split('.')[0]
        self._local_results_filepath = os.path.join(self.session.download_path,
                                                    self.out_filename)

        if write_output_table2log:
            self.query += 'proc print data={0}(obs=30);\n'.format(
                self.out_table_name)

    @property
    def local_results_filepath(self):
        return self._local_results_filepath

    @property
    def log_file(self):
        return self.trunk + '.log'

    @property
    def list_file(self):
        return self.trunk + '.lst'

    @property
    def out_filename(self):
        return self.trunk + '.tsv'

    @property
    def remote_logfile(self):
        """Checks if 'log_file' on remote server and returns it as text.
        :return:
        """
        # TODO: 1. Check log file on remote - if not return message.
        return self.session._get_remote_file(self.log_file)

    @property
    def remote_list_file(self):
        """SAS output written to a list file (.lst)
        :return:
        """
        # TODO: Check list file on remote - if not return message.
        # self.is_file_in_remote_home_dir(self.list_file)
        return self.session._get_remote_file(self.list_file)

    def run(self):
        """Wrapper method to run own query of class BaseQuery()
        :return:
        """
        return NotImplementedError

    def _write2local(self, local_path):
        """Write query to local_path with self.file_name and store its location.
        """
        self.ensure_dir(local_path)
        with open(os.path.join(local_path, self.file_name), 'wb') as fd:
            fd.write(self.query)

        # TODO: Prob better to set this in the inititalisation of the query.
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

    def _write_results2remote(self, libname=None, table_name=None):
        """

        :param filename:
        :return:
        """
        # The library and table name for the results of the query.
        if not table_name:
            table_name = self.out_table_name

        # Allow for library to not be in default output directory.
        query = ''
        if libname:
            query = "libname temp '{0}';".format(libname)

        query += """

        proc export data = {0}
            outfile = "~/{1}"
            dbms = tab
            replace;
            putnames = yes;
        run;
        """.format(table_name, self.out_filename)

        query = BaseQuery(self.session, query, self.trunk + '_temp.sas',
                          self.out_table_name)

        return self.session.run_query(query)

    def write_results2local(self, libname=None):
        """
        :param :
        :return:
        """
        tic = time.time()
        status = self._write_results2remote(libname=libname)

        # Download to local.
        self.session.get_remote_file(self.out_filename)

        if os.path.exists(self.local_results_filepath):
            return [1, time.time()-tic]
        return [0, time.time()-tic]

    def return_dataframe(self):
        """
        :return:
        """
        # Might want to avoid writing the results all the time by checking
        # sizes or something, for safety now just brute force write everything
        # if not os.path.exists(file_path):
        self.write_results2local()
        # TODO: Check file size and chunk??
        return pd.read_csv(self.local_results_filepath, sep='\t',
                           index_col=False)

    def run_query(self, query):
        """

        :param query (BaseQuery):
        :return:
        """
        status, elapsed_time = self.session.run_query(query)
        n_obs = query.get_nobs(query.log_file)
        return status, elapsed_time, n_obs

    def get_nobs(self, log_filename, delimiter=None):
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

        [stdin, stdout, stderr] = self.session.ssh.exec_command(cmd)
        n_obs_raw = (stdout.readlines())
        try:
            return n_obs_raw[0].strip()
        except IndexError:
            # Case where nothing found in log file so accessing [0] returns
            # index error. Can be that observations created, just not logged
            # in format expected, happens with crspmerge for example.
            # TODO: Try to query directly the library with sql as opposed to
            # reading log file.
            return None

    def check_duplicates(self):
        return NotImplementedError

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
