# Various information about WRDS data base structure.


WRDS_DOMAIN = 'wrds.wharton.upenn.edu'

# datasets for which the default is to download the entire data sets at once
_GET_ALL = ['crsp.stocknames', 'comp.company', 'comp.g_company']

AUTOEXEC_TEXT = ("*  The library name definitions below are used by SAS;\n"
    +"*  Assign default libref for WRDS (Wharton Research Data Services);"
    +"\n\n   %include '!SASROOT/wrdslib.sas' ;\n\n\n"
    )

# first_dates is an estimate of the first date YYYYMMDD on        #
# which data is available for a given WRDS dataset.               #
FIRST_DATES = {
    "taq.div": 19930000,
    "taq.mast": 19930000,
    "taq.RGSH": 20050100,
    "taq.cq": 19930104,
    "taq.ct": 19930104,
    "ff.liq_ps": 19620000,
    "ff.factors_daily": 19620000,
    "ff.factors_monthly": 19620000,
    "ff.liq_sadka": 19830000,
    "crsp.msf": 19250000,
    "crsp.dsf": 19250000,
    "crspa.sp500": 19250000,
    "crsp.sp500": 19250000,
    "crspq.portnomap": 20000000,
    "crsp.portnomap": 20000000,
    "crspq.holdings": 20010000,
    "crsp.holdings": 20010000,
    "crspq.front_load": 19610000,
    "crspq.rear_load": 19610000,
    "crsp1.daily_nav_ret": 19980000,
    "crsp.fund_summary2": 19610000,
    "crspa.sp500": 19250000,
    "crspa.cti": 19250000,
    "crspa.bxcalind": 19610000,
    "crspa.tfz_ft": 19610000,
    "crspa.tfz_dly": 19610000,
    "crspa.dreit": 19800000,
    "crspq.dividends": 19600000,
    "crspa.tfz_mth_rf": 19250000,
    "crsp.tfz_mth_rf": 19250000,
    "crspa.mbmdat": 19250000,
    "crsp.cti": 19250000,
    "ibes.det_xepsus": 19890000,
    "ibes.det_xepsint": 19930000,
    "ibes.recddet": 19920000,
    "ibes.det_epsus": 19800000,
    "ibes.det_epsint": 19800000,
    "optionm.opprcd": 19960000,
    "tfn.s12": 19800000,
    "tfn.s34": 19800000,
    "comp.fundq": 19610000,
    "comp.g_fundq": 19870000,
    "comp.secd": 19830000,
    "comp.g_secd": 19850000,
    "comp.idx_daily": 19000000,
    "comp.g_idx_daily": 19700000,
    "comp.inxcst_hist": 19000000,
    "compm.fundq": 19610101
}

# first_guesses is a courser estimate for the dataset provider       #
# crowdsourced improvements to these estimates are welcome.          #
FIRST_DATE_GUESSES = {
    "tfn": 19790000,
    "ibes": 19750000,
    "optionm": 19960000,
    "comp": 19600000,
    "comp.exrt_dly": -1,
    "comp.execcomp": -2,
    "compm": 19610000
}

# date_vars gives the label that each dataset uses for its date variables   #
date_vars = {
    "optionm.opprcd": "date"
}
date_var_guesses = {
    "ibes": "anndats",
    "crsp": "date",
    "tfn": "fdate",
    "comp": "DATADATE",
    "optionm": "effect_date"
}

# dataset_list is a partial list of dataset            #
# labels that can be fetched with get_wrds             #
dataset_list = [
    "bank.all",
    "bvd.ama",
    "bvd.bankscope",
    "risk.directors",
    "risk.governance",
    "sprat.all",
    "doe.all",
    "cboe.all",
    "comp.execucomp",
    "crsp.indices",
    "djones.all",
    "frb.all",
    "fisd.fisd",
    "fisd.naic",
    "phlx.all",
    "trace.all",
    "tfn.insiderdata",
    "taq.div",
    "comp.g_idx_daily",
    "crsp.cti",
    "crspa.cti",
    "crspa.dreit",
    "crspa.tfz_dly",
    "crsp.fund_summary2",
    "comp.inxcst_hist",
    "crsp.sp500",
    "crspa.sp500",
    "comp.secd",
    "comp.g_secd"
]
