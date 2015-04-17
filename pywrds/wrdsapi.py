__author__ = 'cpt'

import os
import json
import re
import datetime
import time
import math
import shutil
import paramiko

from _wrds_db_descriptors import WRDS_DOMAIN, _GET_ALL, FIRST_DATES, \
    FIRST_DATE_GUESSES, AUTOEXEC_TEXT

from pywrds import sshlib
from pywrds import utility as wrds_util
from . import sas_query


class WrdsSession(object):
    """
    Class to hold a WRDS session. Extracts user info and initialises SSH
    connection.

    """

    def __init__(self):
        """

        """
        # if username is not None:
        #    self._username = username

        # Read in the user info from file and assign variables.
        this_file = os.path.abspath(__file__)
        self.user_path = os.path.join(this_file.split('pywrds')[0], 'pywrds')
        self.user_info_filename = os.path.join(self.user_path, 'user_info.txt')
        if os.path.exists(self.user_info_filename):
            with open(self.user_info_filename, 'r') as f:  # r instead of rb for Python3
                # compatibility #
                content = f.read()
                content = content.replace(u'\xe2\x80\x9c', u'"')
                content = content.replace(u'\xe2\x80\x9d', u'"')
                try:
                    self.user_info = json.loads(content)
                except ValueError:
                    print ('WrdsSession warning: user_info.txt file does not '
                            + 'conform to json format.  Please address this '
                            + 'and reload ectools.')
        else:
            print ('WrdsSession warning: Please create a user_info.txt '
                + 'file conforming to the format given in the '
                + 'user_info_example.txt file.')

        self.download_path = os.path.join(self.user_path, 'output')
        if 'download_path' in self.user_info:
            self.download_path = self.user_info['download_path']

        self.wrds_institution = []
        if 'wrds_institution' in self.user_info.keys():
            self.wrds_institution = self.user_info['wrds_institution']

        self.wrds_username =[]
        if 'wrds_username' in self.user_info.keys():
            self.wrds_username = self.user_info['wrds_username']

        if 'last_wrds_download' not in self.user_info.keys():
            self.user_info['last_wrds_download'] = {}
        self.last_wrds_download = self.user_info['last_wrds_download']

        self.now = time.localtime()
        [self.this_year, self.this_month, self.today] = \
            [self.now.tm_year, self.now.tm_mon, self.now.tm_mday]

        # Initialise SSH Client. Only works with key authentication setup.
        # TODO: Generalise login to cases without key authentication
        try:
            [self.ssh, self.sftp] = \
                sshlib.getSSH(ssh=None, sftp=None, domain=WRDS_DOMAIN,
                                username=self.wrds_username)
        except:
            raise Warning("Need to implement login without key authentication")

    def get_ymd_range(self, min_date, dataset, weekdays=1):
        """get_ymd_range(min_date, dataset, weekdays=1) gets a list of
        tuples [year, month, date] over which to iterate in wrds_loop.  Some
        datasets include very large files and need to be queried
        at a monthly or daily frequency to prevent giant files from
        causing problems on the server.

        return ymdrange
        """
        [min_year, min_month, min_day] = self.min_ymd(min_date, dataset)

        ymdrange = []
        years = xrange(min_year, self.now.tm_year+1)
        for year in years:
            frequency = wrds_util.get_loop_frequency(dataset, year)
            if frequency == 'Y':
                new_ymd = [year, 0, 0]
                ymdrange.append(new_ymd)
            elif frequency == 'M':
                new_ymd = [[year, x, 0] for x in range(1, 13)]
                ymdrange = ymdrange + new_ymd
            elif frequency == 'D':
                new_ymd = [[year, x, y] for x in range(1, 13) for y in range(
                    1, 32)]
                new_ymd = wrds_util.fix_weekdays(new_ymd, weekdays)
                ymdrange = ymdrange + new_ymd

        ymdrange = [x for x in ymdrange if x <= [self.this_year,
                                                 self.this_month, self.today]]
        ymdrange = [x for x in ymdrange if x >= [min_year, min_month, min_day]]
        return ymdrange

    def update_user_info(self, numfiles, new_files, fname, dataset, year,
                         month=0, day=0):
        """update_user_info(numfiles, new_files, fname, dataset, year, month=0, day=0)
        amends the user_info file to reflect the most recent download dates
        for wrds files.

        return
        """
        if new_files > 0:
            numfiles = numfiles + new_files
            if 'last_wrds_download' not in self.user_info.keys():
                self.user_info['last_wrds_download'] = {}
            self.user_info['last_wrds_download'][dataset] = \
                year*10000 + month*100 + day
            with open(self.user_info_filename, 'wb') as fd:
                fd.write(json.dumps(self.user_info, indent=4))
        else:
            print ('Could not retrieve: ' + fname)
        return

    def min_ymd(self, min_date, dataset):
        """Finds (year,month,day) at which to start wrds_loop when
        downloading the entirety of a dataset.

        It checks user_info to find what files have already been downloaded.

        :param min_date:
        :param dataset:
        :return [min_year, min_month, min_day]:
        """
        if dataset in _GET_ALL:
            return [-1, -1, -1]

        if 'last_wrds_download' not in self.user_info:
            self.user_info['last_wrds_download'] = {}
        if dataset not in self.user_info['last_wrds_download']:
            if dataset in FIRST_DATES:
                self.user_info['last_wrds_download'][dataset] = FIRST_DATES[
                    dataset]
            else:
                self.user_info['last_wrds_download'][dataset] = 18000000

        if not isinstance(min_date, (int, float)):
            min_date = 0

        if min_date == 0:
            min_date = self.user_info['last_wrds_download'][dataset]
            min_date = str(min_date)
            if not min_date.isdigit() or len(min_date) != 8:
                min_date = 0
                print ('user_info["last_wrds_download"]["' + dataset + '"]='
                    + min_date + ' error, should be an eight digit integer.')
            min_year = int(float(min_date[:4]))
            min_month = int(float(min_date[4:6]))
            min_day = int(float(min_date[6:]))
            if min_month == min_day == 0:
                min_year += 1
            elif min_day == 0:
                min_month += 1
                if min_month == 13:
                    min_month = 1
                    min_year += 1
            else:
                min_day += 1
                try:
                    wday = datetime.date(min_month, min_month,
                                         min_day).weekday()
                except:
                    min_day = 1
                    min_month += 1
                    if min_month == 13:
                        min_month = 1
                        min_year += 1

        if min_date != 0:
            if min_date < 1880:
                min_day = 0
                min_month = 0
                min_year = 1880
                print ('Setting min_year = 1880.  This will result in '
                    +'many empty data files and unnecessary looping.  '
                    +'This can be prevented by a) inputting a higher '
                    +'min_date or b) finding the first date at which '
                    +'this dataset is available on WRDS and letting '
                    +'Brock know so he can update the code appropriately.')
            elif min_date < 2050:
                min_day = 0
                min_month = 0
                min_year = int(min_date)
            elif 188000 < min_date < 1880000:
                min_month = min_date%100
                min_year = (min_date - (min_date%100))/100
            elif min_date < 20500000:
                min_day = min_date%100
                min_month = (min_date%10000 - min_day)/100
                min_year = (min_date - (min_date%10000))/10000

        if min_date == 0:
            if dataset in FIRST_DATES.keys():
                min_day = FIRST_DATES[dataset]%100
                min_month = ((FIRST_DATES[dataset] - min_day)%10000)/100
                min_year = (FIRST_DATES[dataset] - 100*min_month -
                            min_day) / 10000
            elif any(re.search(x, dataset) for x in FIRST_DATE_GUESSES.keys()):
                key = [x for x in FIRST_DATE_GUESSES.keys()
                    if re.search(x, dataset)][0]
                if dataset in FIRST_DATE_GUESSES.keys():
                    key = dataset
                    if FIRST_DATE_GUESSES[key] == -1:
                        return [-1, -1, -1]
                min_day = FIRST_DATE_GUESSES[key]%100
                min_month = ((FIRST_DATE_GUESSES[key]-min_day)%10000)/100
                min_year = (FIRST_DATE_GUESSES[key]-100*min_month-min_day)/10000
            else:
                min_day = 0
                min_month = 0
                min_year = 1880

        return [min_year, min_month, min_day]

    def setup_wrds_key(self):
        """setup_wrds_key() sets up a key-based authentication on
        the wrds server, so that the user can log in without a
        password going forward.

        return [ssh, sftp]
        """
        if not self.wrds_username:
            print('setup_wrds_key() cannot run until wrds_username is '
                +'specified in the user_info.txt file.')
            return [None, None]
        institution = self.get_wrds_institution()
        return [self.ssh, self.sftp]

    def get_wrds_institution(self):
        """get_wrds_institution(ssh, sftp) gets the institution associated
        with the user's account on the wrds server.

        return institution_path
        """
        if not self.sftp:
            return None
        try:
            wrds_path = self.sftp.normalize(path='.')
        except IOError:
            print ('sftp cannot resolve a path on the wrds server')
            return None
        institution_path = re.sub('/home/', '', wrds_path).split('/')[0]
        if self.wrds_institution != institution_path:
            if self.wrds_institution == []:
                self.wrds_institution = institution_path
                self.user_info['wrds_institution'] = self.wrds_institution
                with open(self.user_info_filename, 'wb') as fd:
                    fd.write(json.dumps(self.user_info, indent=4))
            else:
                print ('user_info["wrds_institution"] does not '
                    + 'match the directory "' + institution_path + '" '
                    + 'found on the wrds server.  '
                    + 'This mismatch may cause errors '
                    + 'in the download process.')
        return institution_path

    def get_wrds(self, dataset, Y, M=0, D=0, recombine=1):
        """Remotely download a file from the WRDS server. For example,
        the command

        x = get_wrds('crsp.msf', 2010, 6)

        will log in to the WRDS server, issue a query to generate
        a tab-separated(*) file containing the entire CRSP Monthly
        Stock File dataset for June 2010, then download that file
        to your download_path (which you can edit in the user
        information section above).  The output x is a pair
        [indicator,elapsed_time] where indicator is a one if the
        download was successful, zero otherwise.

        The arguments Y, M, D stand for Year, Month, Day, respectively.
        Ommitting the month argument

        get_wrds(dataset_name, year)

        will retrieve a single file for the entire year.

        (*) Tab-separated files (tsv) tend to work slightly
        better than comma-separated files (csv) because sometimes
        company names have commas e.g. Company Name, Inc.

        :param dataset:
        :param Y:
        :param M:
        :param D:
        :param recombine:
        :return [numfiles, total_rows, time_elapsed]:
        """
        tic = time.time()
        keep_going = 1
        [startrow, numfiles, total_rows, tic] = [1, 0, 0, time.time()]
        rows_per_file = wrds_util.rows_per_file_adjusted(dataset)
        [dset2, outfile] = wrds_util.fix_input_name(dataset, Y, M, D, [])

        # Check if output file in local dir, if not send request.
        if os.path.exists(os.path.join(self.download_path, outfile)):
            keep_going = 0
        while keep_going:
            R = [startrow, startrow - 1 + rows_per_file]
            [dset2, outfile] = wrds_util.fix_input_name(dataset, Y, M, D, R)

            if not os.path.exists(os.path.join(self.download_path, outfile)):
                [keep_going, dt] = self._get_wrds_chunk(dataset, Y, M, D, R)

            if keep_going > 0:
                numfiles += 1
                if os.path.exists(os.path.join(self.download_path, outfile)):
                    log_lines = wrds_util.get_numlines_from_log(
                        outfile, dname=self.download_path)
                    numlines = wrds_util.get_numlines(os.path.join(
                        self.download_path, outfile))
                    if log_lines > numlines:
                        print('get_wrds error: file '
                            +outfile+' has '+ str(numlines)
                            +' lines, but '+ str(log_lines)
                            +' were expected.')
                        keep_going = 0

                    total_rows += numlines
                    if numlines < rows_per_file:
                        keep_going = 0

                    if log_lines == numlines < rows_per_file:
                        keep_going = 0
                        if not (log_lines == -1 or log_lines == numlines):
                            print('get_wrds warning: '
                                +'log_lines = '+str(log_lines))
                        if startrow == 1:
                            subfrom = 'rows1to' + str(rows_per_file)
                            newname = re.sub(subfrom, '', outfile)
                            newp2f = os.path.join(self.download_path, newname)
                            oldp2f = os.path.join(self.download_path, outfile)
                            os.rename(oldp2f, newp2f)
                        else:
                            subfrom = 'to'+str(R[-1])
                            subto = 'to'+str(R[0] - 1 + numlines)
                            newname = re.sub(subfrom, subto, outfile)
                            oldp2f = os.path.join(self.download_path, outfile)
                            newp2f = os.path.join(self.download_path, newname)
                            os.rename(oldp2f, newp2f)
                        if recombine == 1:
                            subfrom = 'rows[0-9]*to[0-9]*\.tsv'
                            recombine_name = re.sub(subfrom, '', outfile)
                            wrds_util.recombine_files(recombine_name,
                                             dname=self.download_path)
                    else:
                        startrow += rows_per_file
                        newname = outfile

                else:
                    keep_going = 0

        return [numfiles, total_rows, time.time()-tic]

    def _get_wrds_chunk(self, dataset, Y, M=0, D=0, R=[]):
        """_get_wrds_chunk(dataset, Y, M=0, D=0, rows=[])

        Some files requested by get_wrds are too large to fit
        in a user's allotted space on the wrds server.  For these
        files, get_wrds will split the request into multiple
        smaller requests to retrieve multiple files and run each
        of them through _get_wrds_chunk.  If the argument
        "recombine" is set to its default value of 1, these files
        will be recombined once the loop completes.

        return [success, ssh, sftp, time_elapsed]
        """
        tic = time.time()
        [sas_file, outfile, dataset] = \
            sas_query.wrds_sas_script(self.download_path, dataset, Y, M, D, R)
        log_file = re.sub('\.sas$', '.log', sas_file)

        put_success = self._put_sas_file(outfile, sas_file)
        exit_status = self._sas_step(sas_file, outfile)
        exit_status = self._handle_sas_failure(exit_status, outfile, log_file)

        if exit_status in [0, 1]:
            [fdict] = self._try_listdir('.', WRDS_DOMAIN, self.wrds_username)
            file_list = fdict.keys()
            if outfile not in file_list:
                print('exit_status in [0, 1] suggests SAS succeeded, '
                    +'but the desired output_file "'
                    +outfile+'" is not present in the file list:')
                print(file_list)

            else:
                remote_size = self._wait_for_sas_file_completion(outfile)
                [get_success, dt] = self._retrieve_file(outfile, remote_size)
                local_size \
                    = wrds_util.wait_for_retrieve_completion(outfile, get_success)
                compare_success = \
                    self._compare_local_to_remote(outfile, remote_size,
                                                  local_size)

        [got_log] = self._get_log_file(log_file, sas_file)
        checkfile = os.path.join(self.download_path, outfile)
        if os.path.exists(checkfile) or exit_status == 0:
            return [1, time.time()-tic]
        return [0, time.time()-tic]

    def _rename_after_download(self):
        return NotImplementedError

    def wrds_loop(self, dataset, min_date=0, recombine=1):
        """wrds_loop(dataset, min_date=0, recombine=1)
        executes get_wrds(database_name,...) over all years and
        months for which data is available for the specified
        data set.  File separated into chunks for downloading
        will be recombined into their original forms if
        recombine is set to its default value 1.

        return [numfiles, time_elapsed]
        """
        tic = time.time()
        [numfiles, numlines, numlines0] = [0, 0, 0]
        [min_year, min_month, min_day] = self.min_ymd(min_date, dataset)
        flist = os.listdir(self.download_path)

        if [min_year, min_month, min_day] == [-1, -1, -1]:
            Y = 'all'
            get_output = self.get_wrds(dataset, Y, M=0, D=0, recombine=recombine)
            [new_files, total_lines, dt] = self.get_output
            if new_files > 0:
                numfiles = numfiles + 1
            return [numfiles, time.time()-tic]


        for ymd in self.get_ymd_range(min_date, dataset, 1):
            [Y, M, D] = ymd
            [dset2, outfile] = wrds_util.fix_input_name(dataset, Y, M, D, [])
            if outfile in flist:
                continue
            get_output = self.get_wrds(dataset, Y, M=M, D=D, recombine=recombine)
            [new_files, total_lines, dt] = self.get_output

            numfiles = numfiles + new_files
            self.update_user_info(numfiles, new_files, fname=outfile,
                                  dataset=dataset, year=Y, month=M, day=D)

        return [numfiles, time.time()-tic]

    def _put_sas_file(self, outfile, sas_file):
        """_put_sas_file(outfile, sas_file) puts the sas_file
        in the appropriate directory on the wrds server, handling
        several common errors that occur during this process.

        It removes old files which may interfere with the new
        files and checks that there is enough space in the user
        account on the wrds server to run the sas command.

        Finally it checks that the necessary autoexec.sas files
        are present in the directory.

        return put_success_boolean
        """
        [fdict] = self._try_listdir('.', WRDS_DOMAIN, self.wrds_username)
        initial_files = fdict.values()

        old_export_files = [x for x in initial_files
            if re.search('wrds_export.*sas$', x.filename)
            or re.search('wrds_export.*log$', x.filename)
            or x.filename == sas_file]
        for old_file in old_export_files:
            try:
                self.sftp.remove(old_file.filename)
            except (IOError, EOFError, paramiko.SSHException):
                pass
            initial_files.remove(old_file)

        pattern = '[0-9]*rows[0-9]+to[0-9]+\.tsv$'
        old_outfiles = [x for x in initial_files
            if re.sub(pattern, '', x.filename) == re.sub(pattern, '', outfile)]

        for old_file in old_outfiles:
            try:
                self.sftp.remove(old_file.filename)
            except (IOError, EOFError, paramiko.SSHException):
                pass
            initial_files.remove(old_file)
            ## see if the file is something you                ##
            ## actually want before deleting it out of hand    ##

        file_sizes = [initial_file.st_size for initial_file in initial_files]
        total_file_size = sum(file_sizes)
        if total_file_size > 5*10**8:
            MBs = int(float(total_file_size)/1000000)
            print('You are using approximately '+str(MBs)
                +' megabytes of your 1 GB'
                +' quota on the WRDS server.  This may cause '
                +'WrdsSession.get_wrds to operate'
                +' incorrectly.  The files present are: ')
            print([x.filename for x in initial_files])

        auto_names = ['autoexec.sas', '.autoexecsas']
        autoexecs = [x.filename for x in initial_files if x.filename in auto_names]
        if autoexecs == ['.autoexecsas']:
            # if 'autoexec.sas' is not present, the sas program will fail   #
            # a backup copy is stored by default in .autoexecsas            #
            ssh_command = 'cp .autoexecsas autoexec.sas'
            [exec_succes, stdin, stdout, stderr] = \
                self._try_exec(ssh_command, WRDS_DOMAIN, self.wrds_username)

        elif autoexecs == ['autoexec.sas']:
            ssh_command = 'cp autoexec.sas .autoexecsas'
            [exec_succes, stdin, stdout, stderr] = self._try_exec(
                ssh_command, WRDS_DOMAIN, self.wrds_username)

        elif autoexecs == []:
            with open('autoexec.sas', 'wb') as fd:
                fd.write(AUTOEXEC_TEXT)
            local_path = 'autoexec.sas'
            remote_path = 'autoexec.sas'
            [put_success] = self._try_put(local_path, remote_path,
                                          WRDS_DOMAIN, self.wrds_username)
            ssh_command = 'cp autoexec.sas .autoexecsas'
            [exec_succes, stdin, stdout, stderr] = self._try_exec(ssh_command,
                                                              WRDS_DOMAIN,
                                                              self.wrds_username)
            os.remove('autoexec.sas')

        local_path = os.path.join(self.download_path, sas_file)
        remote_path = sas_file
        [put_success] = self._try_put(local_path, remote_path,
                                            WRDS_DOMAIN, self.wrds_username)
        #[put_success, numtrys, max_trys] = [0, 0, 3]
        #while put_success == 0 and numtrys < max_trys:
        #	try:
        #		sftp.put(local_path, sas_file)
        #		put_success = 1
        #	except (paramiko.SSHException,IOError,EOFError):
        #		[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
        #		numtrys += 1

        return put_success

    def _sas_step(self, sas_file, outfile):
        """_sas_step(ssh, sftp, sas_file, outfile) wraps the running of
        the sas command (_run_sas_command) with retrying and
        re-initializing the network connection if necessary.

        return exit_status
        """
        [sas_completion, num_sas_trys, max_sas_trys] = [0, 0, 3]
        while sas_completion == 0 and num_sas_trys < max_sas_trys:
            exit_status = self._run_sas_command(sas_file, outfile)
            num_sas_trys += 1
            sas_completion = 1

            if exit_status in [42, 104]:
                # 42 = network read failed                 #
                # 104 = connection reset by peer           #
                # TODO: Deal with reinitiating a session - this will break.
                sas_completion = 0
                # [ssh, sftp] = getSSH(ssh, sftp, domain=WRDS_DOMAIN,
                # username=_uname)
                if not self.sftp:
                    return exit_status

                [fdict] = self._try_listdir('.', WRDS_DOMAIN, self.wrds_username)

                if outfile in fdict.keys():
                    exit_status = 0
                    sas_completion = 1

                elif 'log_file' in fdict.keys():
                    exit_status = -1
                    sas_completion = 1
        return exit_status

    def _run_sas_command(self, sas_file, outfile):
        """Executes the sas script sas_file on the wrds server and waits for
        an exit status to be returned.

        :param sas_file:
        :param outfile:
        :return exit_status:
        """
        sas_command = ('sas -noterminal '+ sas_file)
        [stdin,stdout,stderr] = self.ssh.exec_command(sas_command)
        [exit_status, exit_status2, waited, maxwait] = [-1, -1, 0, 1200]
        while exit_status == -1 and waited < maxwait:
            time.sleep(10)
            waited += 10
            exit_status = stdout.channel.recv_exit_status()

        if waited >= maxwait:
            print('get_wrds stopped waiting for SAS '
                +'completion at step 1: '+outfile)
        return exit_status

    def _handle_sas_failure(self, exit_status, outfile, log_file):
        """Checks sas exit status returned by wrds server and responds
        appropriately to any statuses other than success.

        :param exit_status:
        :param outfile:
        :param log_file:
        :return exit_status:
        """
        real_failure = 1
        [fdict] = self._try_listdir('.', WRDS_DOMAIN, self.wrds_username)

        if exit_status == 2 and log_file in fdict.keys():
            fd = self.sftp.file(log_file)
            logcontent = fd.read()
            fd.close()
            if re.search('error: file .* does not exist.', logcontent,
                         flags=re.I):
                real_failure = 0

        if exit_status not in [0, 1] and real_failure == 1:
            # 1 is "SAS system issued warnings", non-fatal    #

            if outfile in fdict.keys():
                print('SAS is apparently returning an incorrect '
                    + 'exit status: ' + str(exit_status)+', '+outfile+'.  '
                    + 'ectools is downloading the file for user inspection.')
                remote_path = outfile
                local_path = os.path.join(self.download_path, outfile)
                [get_success, dt] = \
                    self._try_get(domain=WRDS_DOMAIN, username=self.wrds_username,
                                  remote_path=remote_path, local_path=local_path)
                if get_success == 0:
                    print('File download failure.')

            else:
                print('get_wrds failed on file "' + outfile + '"\n'
                    + 'exit_status = ' + str(exit_status) + '\n'
                    + 'For details, see log file "' + log_file + '"')

        return exit_status

    def _wait_for_sas_file_completion(self, outfile):
        """Checks the size of the file outfile produced on the wrds server
        within get_wrds.

        Until it observes two successive measurements with the same file
        size, it infers that the sas script is still writing the file.

        :param outfile:
        :return remote_size:
        """
        [measure1, measure2, mtime, waited2, maxwait2] = [0, 1, time.time(), 0, 1200]
        while self.sftp and ((waited2 < maxwait2)
            and (measure1 != measure2 or (time.time() - mtime <= 10))):
            measure1 = measure2
            time.sleep(10)
            waited2 += 10
            try:
                output_stat = self.sftp.stat(outfile)
                measure2 = output_stat.st_size
                mtime = output_stat.st_mtime
            except (IOError, EOFError, paramiko.SSHException):
                raise NotImplementedError
                # [ssh, sftp] = getSSH(ssh, sftp, domain=WRDS_DOMAIN,
                # username=_uname)

        if waited2 >= maxwait2:
            print(['get_wrds stopped waiting for SAS completion at step 2',
                measure1, measure2, mtime])
            measure1 = 0
            # should i remove the file in this case?
        remote_size = measure1

        return remote_size

    def _retrieve_file(self, outfile, remote_size):
        """Retrieves the outfile produced on the wrds server in
        get_wrds, including correct handling of several common network errors.

        :param outfile:
        :param remote_size:
        :return get_success:
        """
        tic = time.time()
        if remote_size == 0:
            return [0, time.time()-tic]
        if remote_size >= 10**7:
            # skip messages for small files        #
            print('starting retrieve_file: '+outfile
                +' ('+repr(remote_size)+') bytes')

        vfs = os.statvfs(self.download_path)
        free_local_space = vfs.f_bavail*vfs.f_frsize
        if remote_size > free_local_space:
            print('get_wrds cannot download file '+outfile+', only '
            +str(free_local_space)+' bytes available on drive for '
            +str(remote_size)+'-byte file.')
            return [0, time.time()-tic]

        [get_success, numtrys, maxtrys] = [0, 0, 3]
        remote_path = ('/home/' + self.wrds_institution + '/' +
                       self.wrds_username + '/' + outfile)
        write_file = '.' + outfile + '--writing'
        local_path = os.path.join(os.path.expanduser('~'), write_file)
        [get_success, dt] = \
            self._try_get(domain=WRDS_DOMAIN, username=self.wrds_username,
                          remote_path=remote_path, local_path=local_path)

        print('retrieve_file: '+repr(outfile)
            +' ('+repr(remote_size)+' bytes) '
            +' time elapsed='+repr(time.time()-tic))

        return [get_success, time.time()-tic]

    def _compare_local_to_remote(self, outfile, remote_size, local_size):
        """Compares the size of the file "outfile" downloaded (local_size) to
        the size of the file as listed on the server (remote_size) to
        check that the download completed properly.

        :param outfile:
        :param remote_size:
        :param local_size:
        :return compare_success (bool):
        """
        compare_success = 0
        write_file = '.' + outfile + '--writing'
        local_path = os.path.join(os.path.expanduser('~'), write_file)
        if remote_size == local_size != 0:
            [exec_succes, stdin, stdout, stderr] = \
                self._try_exec('rm ' + outfile, WRDS_DOMAIN, self.wrds_username)
            to_path = os.path.join(self.download_path, outfile)
            shutil.move(local_path, to_path)
            compare_success = 1

        elif local_size != 0:
            print(['remote_size != local_size', outfile, remote_size, local_size])
            log_size = math.log(local_size, 2)
            if log_size == int(log_size):
                print('The error appears to involve '
                    +'the download stopping at 2^' + repr(log_size) + ' bytes.')
            error_file = '.' + outfile + '--size_error'
            from_file = os.path.join(os.path.expanduser('~'), error_file)
            to_file = os.path.join(self.download_path, outfile)
            shutil.move(from_file, to_file)
            compare_success = 0

        return compare_success

    def _get_log_file(self, log_file, sas_file):
        """Attempts to retrieve SAS log file generated by _get_wrds_chunk from
        the WRDS server.

        Also removes the sas_file from the local directory, though strictly
        speaking this belongs in a separate function.

        :param log_file:
        :param sas_file:
        :return success (bool):
        """
        success = 0
        remote_path = ('/home/' + self.wrds_institution + '/' +
                       self.wrds_username + '/' + log_file)
        local_path = os.path.join(self.download_path, log_file)
        [success, dt] = \
            self._try_get(domain=WRDS_DOMAIN, username=self.wrds_username,
                          remote_path=remote_path, local_path=local_path)
        [exec_succes, stdin, stdout, stderr] = \
            self._try_exec('rm ' + sas_file, WRDS_DOMAIN, self.wrds_username)
        [exec_succes, stdin, stdout, stderr] = \
            self._try_exec('rm wrds_export*', WRDS_DOMAIN, self.wrds_username)

        saspath = os.path.join(self.download_path, sas_file)
        if os.path.exists(saspath):
            os.remove(saspath)
        return [success]

    def find_wrds(self, filename):
        """Query WRDS for a list of tables available from dataset_name.

        E.g. setting dataset_name = 'crsp' returns a file with a list of names
        including "dsf" (daily stock file) and "msf" (monthly stock file).

        :param filename:
        :return: [file_list, ssh, sftp]
        """
        tic = time.time()
        local_sas_file = os.path.join(self.download_path, 'wrds_dicts.sas')
        with open(local_sas_file, 'wb') as fd:
            fd.write('\tproc sql;\n')
            fd.write('\tselect memname\n')
            # optional: "select distinct memname"   #
            fd.write('\tfrom dictionary.tables\n')
            fd.write('\twhere libname = "' + filename.upper() + '";\n')
            fd.write('\tquit;\n')
        for fname in ['wrds_dicts.sas', 'wrds_dicts.lst', 'wrds_dicts.log']:
            try:
                self.sftp.remove(fname)
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except:  # TODO: Handle case when file doesn't exist explicitly.
                pass

        [put_success] = self._try_put(local_sas_file, 'wrds_dicts.sas',
                                      WRDS_DOMAIN, self.wrds_username)

        sas_command = 'sas -noterminal wrds_dicts.sas'

        [stdin, stdout, stderr] = self.ssh.exec_command(sas_command)
        exit_status = -1
        while exit_status == -1:
            time.sleep(10)
            exit_status = stdout.channel.recv_exit_status()

        local_path = os.path.join(self.download_path, filename + '_dicts.lst')
        remote_path = ('/home/' + self.wrds_institution + '/' +
                       self.wrds_username + '/wrds_dicts.lst')
        [fdict] = self._try_listdir('.', WRDS_DOMAIN, self.wrds_username)
        remote_list = fdict.keys()

        if exit_status in [0, 1] and 'wrds_dicts.lst' in remote_list:
            [get_success, dt] = \
                self._try_get(domain=WRDS_DOMAIN, username=self.wrds_username,
                          remote_path=remote_path, local_path=local_path)
        else:
            print('find_wrds did not generate a wrds_dicts.lst '
                + 'file for input: ' + repr(filename))
        try:
            self.sftp.remove('wrds_dicts.sas')
        except (IOError, EOFError, paramiko.SSHException):
            pass
        os.remove(local_sas_file)

        flist = []
        if os.path.exists(local_path):
            with open(local_path, 'rb') as fd:
                flist = fd.read().splitlines()
            flist = [x.strip() for x in flist]
            flist = [x for x in flist if x != '']
            dash_line = [x for x in range(len(flist)) if flist[x].strip('- ') == '']
            if dash_line:
                dnum = dash_line[0]
                flist = flist[dnum:]

        return [flist]

    def _try_put(self, local_path, remote_path, domain, username, ports=[22]):
        """Transfers file from local_path to remote_path using the sftp client.

        TODO: Reinitiating the ssh connection if needbe.

        :param local_path:
        :param remote_path:
        :param domain:
        :param username:
        :param ports:
        :return success:
        """
        local_stat = os.stat(local_path)
        [success, n_tries, max_tries] = [0, 0, 3]
        while not success and n_tries < max_tries:
            try:
                remote_attrs = self.sftp.put(local_path, remote_path)
                # Check file transferred is same as local version
                if remote_attrs.st_size == local_stat.st_size:
                    success = 1
            except KeyboardInterrupt:
                try:
                    self.sftp.remove(remote_path)
                except:
                    pass
                raise KeyboardInterrupt
            except (IOError, EOFError, paramiko.SSHException):
                # TODO: Handle sftp error, try to reconnect.
                try:
                    self.sftp.remove(remote_path)
                except (IOError, EOFError, paramiko.SSHException):
                    pass
            n_tries += 1
        return [success]

    def _try_get(self, domain, username, remote_path, local_path, ports=[22]):
        """Tries three times to download file from remote_path to local_path
        using the sftp client.

        TODO: If a connection error occurs, it is re-established.

        Does *not* check that the remote file exists, that the local_path is
        not already in use, or that there is enough space free on the local
        disk to complete the download.

        :param domain:
        :param username:
        :param remote_path:
        :param local_path:
        :param ports:
        :return [success (bool), time_elapsed]:
        """
        tic = time.time()
        [success, n_tries, max_tries] = [0, 0, 3]
        while not success and n_tries < max_tries:
            try:
                self.sftp.get(remotepath=remote_path, localpath=local_path)
                success = 1
            except (paramiko.SSHException, paramiko.SFTPError, IOError,
                    EOFError):
                if os.path.exists(local_path):
                    os.remove(local_path)
                # TODO: Handle sftp error, try to reconnect.
                n_tries += 1
            except KeyboardInterrupt:
                if os.path.exists(local_path):
                    os.remove(local_path)
                raise KeyboardInterrupt

        return [success, time.time()-tic]

    def _try_exec(self, command, domain, username, ports=[22]):
        """

        :param command:
        :param domain:
        :param username:
        :param ports:
        :return [success, stdin, stdout, stderr]:
        """
        [success, n_tries, max_tries] = [0, 0, 3]
        [stdin, stdout, stderr] = [None, None, None]
        while not success and n_tries < max_tries:
            try:
                [stdin, stdout, stderr] = self.ssh.exec_command(command)
                success = 1
            except (IOError, EOFError, paramiko.SSHException):
                # TODO: Handle sftp error, try to reconnect.
                n_tries += 1

        return [success, stdin, stdout, stderr]

    def _try_listdir(self, remote_dir, domain, username, ports=[22]):
        """Tries three times to get a a list of files and their attributes
        from the directory remote_dir on the remote server.

        TODO: reinitiating the ssh connection if needbe.

        :param remote_dir:
        :param domain:
        :param username:
        :param ports:
        :return (dict): {filename: [attributes]} across files in the remote
                        directory
        """
        remote_list = []
        [success, n_tries, max_tries] = [0, 0, 3]
        while not success and n_tries < max_tries:
            try:
                remote_list = self.sftp.listdir_attr(remote_dir)
                success = 1
            except (IOError, EOFError, paramiko.SSHException):
                # TODO: Handle sftp error
                n_tries += 1

        return {x.filename: x for x in remote_list}


