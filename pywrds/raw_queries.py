__author__ = 'cpt'
import os


class BaseQuery(object):
    """Container class for a sas query and its filename.
    """
    def __init__(self, query, query_name):
        self.query = query
        self.file_name = query_name
        self._local_path = ''

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

    def _remove_file_from_local(self):
        """Remove the file from the local path.
        """
        # self._local_path, self.file_name
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


def eg_query_crsp():
    query = """
    /* Year Range */
    %LET FIRST = 1965;
    %LET LAST = 1968;
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


