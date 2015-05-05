__author__ = 'cpt'
from query import BaseQuery

def eg_query_crsp(session):
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
    return BaseQuery(session, query, 'test_crsp.sas')


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


