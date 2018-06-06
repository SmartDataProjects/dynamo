from dynamo.fileop.fileop import FileOperation
from dynamo.utils.parallel import Map

class SingleNodeFileOperation(FileOperation):
    def __init__(self, config):
        FileOperation.__init__(self, config)

        self.nthreads = config.nthreads

        self.siteNoAccess = {}

    def copy_file(self, destination, fileName, sources):
        ## NOT DEFINED
        destination_site = inventory.sites[destination]
        ## NOT DEFINED

        mysql_local = MySQL(**config.mysqlregistry.db_params)

        print ""
        print ', '.join(sources) + " .. starting transfer at " + datetime.datetime.now().strftime("%I:%M %p")
        destin_full = destination_site.to_pfn(fileName, self.protocol)

        for oneSource in sources:
            ## NOT DEFINED
            source_site = inventory.sites[oneSource]
            ## NOT DEFINED

            source_full = source_site.to_pfn(fileName, self.protocol)

            kill_proc = lambda p: p.kill()
            cmd = 'gfal-copy -p -f ' + source_full + ' ' + destin_full
	    print cmd
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       bufsize=4096,shell=True)
            timer = Timer(1080, kill_proc, [process])

            try:
                timer.start()
                strout, error = process.communicate()
                if process.returncode != 0:
                    print " Received non-zero exit status: " + str(process.returncode)
                    raise Exception(" FATAL -- Call to gfal-copy failed, stopping")
                else:
                    print ".. copied ........................."  
                    sql = "update transfer_queue set status='done' where file='" + fileName + "'"
                    sql = sql + " and site_to='" + destination + "'"
                    mysql_local.query(sql)
                    print ' .. finished at ' + datetime.datetime.now().strftime("%I:%M %p")
                    return (True,destination,fileName)

            except Exception, e:
                print " !!!!!!!!!!!!---------!!!!!!!!!!!!"
                print ' .. bailed out at ' + datetime.datetime.now().strftime("%I:%M %p")
            
                if 'Permission' in str(e):
                    print "got deny permission, aborting"
                elif 'No such file' in str(e):
                    print "file does not exist at source, aborting"
                else:
                    print str(e)
            finally:
                timer.cancel()

        sql = "update transfer_queue set status='failed' where file='" + fileName + "'"
        sql = sql + " and site_to='" + destination + "'"
        mysql_local.query(sql)
        return (False,destination,fileName)

    def delete_files(self):
        gfal2.set_verbose(gfal2.verbose_level.warning)
        startOver = True
        while startOver == True:
            print "--- will do " + str(len(arguments)) + " items"
            try:
                parallel_exec(self.remove_file, arguments, 5, self.nthreads, False, 0)
                startOver = False
            except NameError:
                for siteName in self.siteNoAccess:
                    if self.siteNoAccess[siteName] > 100:
                        print ".. removing site " + siteName + " from action"
                        y = [s for s in arguments if s[0] != siteName]
                        arguments = y


    def remove_from_db(self,fileName,siteName,sql_handle):
        sql = "delete from deletion_queue where file='" + fileName + "'"
        sql = sql + " and site='" + siteName + "'"
        print sql
        sql_handle.query(sql)


    def remove_file(self,siteName,fileName):
        mysql_local = MySQL(**config.mysqlregistry.db_params)
        
        attempts = 1
        suspectDir = False
        if '.root' not in fileName:
            suspectDir = True

        ### NOT DEFINED
        site = inventory.sites[siteName]
        ### NOT DEFINED

        fullpath = site.to_pfn(fileName, self.protocol)
        print fullpath
        while(True):
            ctx = gfal2.creat_context()
            try:
                if attempts > 1 or suspectDir:
                    r = ctx.rmdir(fullpath)
                else:
                    r = ctx.unlink(fullpath)
                del ctx
                print " --- deleted " + fullpath
                sql = "update deletion_queue set status='done' where file='" + fileName + "'"
                sql = sql + " and site='" + siteName + "'"
                mysql_local.query(sql)
                if siteName not in self.siteNoAccess:
                    self.siteNoAccess[siteName] = 0
                    self.siteNoAccess[siteName] -= 1
                return (True,siteName,fileName)
            except Exception, e:
                print " !!!!!!!!!!!!---------!!!!!!!!!!!!"
                print str(e)
                if 'Permission' in str(e) or 'SRM_AUTHORIZATION_FAILURE' in str(e):
                    print "got deny permission, aborting"
                    del ctx

                    if siteName not in self.siteNoAccess:
                        self.siteNoAccess[siteName] = 0
                    self.siteNoAccess[siteName] += 1
                    return (False,siteName,fileName)
                elif 'No such file' in str(e):
                    print "file does not exist, cleaning and aborting"
                    self.remove_from_db(fileName,siteName,mysql_local)
                    del ctx
                    return (False,siteName,fileName)
                elif 'path is a dir' in str(e):
                    print "dealing with directory"
                    suspectDir = True
                elif 'Is a directory' in str(e):
                    print "dealing with directory"
                    suspectDir = True
                    
                else:
                    try:
                        r = ctx.listdir(fullpath)
                        if len(r) > 0:
                            print ' .. non empty dir, delete from database'
                            self.remove_from_db(fileName,siteName,mysql_local)
                        return (True,siteName,fileName)
                    except Exception, ee:
                        if 'No such file' in str(ee):
                            print ' .. will delete from database'
                            self.remove_from_db(fileName,siteName,mysql_local)
                        return (True,siteName,fileName)

                del ctx
                attempts += 1
           
            if attempts > 2:
                return (False,siteName,fileName)
