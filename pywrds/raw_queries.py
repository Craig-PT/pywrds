__author__ = 'cpt'
from query import BaseQuery


def eg_nobs_crsp(session):
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
    return BaseQuery(session, query, 'n_crsp.sas')


class CrspQuery(object):

    def __init__(self, lib_name=None, lib_dir=None):
        """Create SQL query to CRSP.

        """
        if lib_name is not None:
            self.lib_name = lib_name
        if lib_dir is not None:
            self.lib_dir = lib_dir

    def run_crsp_query(self, session):

        query = eg_query_crsp(session)
        return query.run_query(query)

    def get_file(self, session):
        """Writes the query to a .tsv

        :param query:
        :return:
        """
        query = eg_query_crsp(session)
        # 1. Check lib exists, if not run query.

        # 2. Write results to .tsv on remote, and Transfer file remote -> local.
        success, elapsed_time = query.write_results2local()
        return success


