# Generic class to connect python to sql DB
import psycopg2

class ConnectionFunctions:

    def __init__(self):
        self.conn = []
        self.cur = []

    # generic connection functions
    def connect(self, host, port, dbname, user, password=None):
        if password is None:
            connstring="host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user
        else:
            connstring="host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user + " password=" + password

        self.conn = psycopg2.connect(connstring)
        self.cur = self.conn.cursor()
        self.conn.set_isolation_level(0)

    def loadFunctions(self, sqlname):
        exestring = open(sqlname, 'r').read()
        self.cur.execute(exestring)

    def stopConn(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def initialiseTable(self, tablename, tablecols):
        dropstring = "drop table if exists " + tablename
        self.cur.execute(dropstring)
        createstring = "create table " + tablename + tablecols
        self.cur.execute(createstring)

    def droptable(self, tablename):
        dropstring = "drop table if exists " + tablename
        self.cur.execute(dropstring)
            
    # calling procs
    def callUserFunc(self, funcname, input):
        self.cur.callproc(funcname, input)
        retval=self.cur.fetchall()
        return retval

    def executeandfetch(self, execstring):
        self.cur.execute(execstring)
        retval=self.cur.fetchall()
        return retval

    def executeandfetchnumber(self, execstring):
        self.cur.execute(execstring)
        retval=self.cur.fetchall()
        return retval[0][0]

    def execute(self, execstring):
        self.cur.execute(execstring)

    # some common user functions
    def getUnique(self, colname, tablename):
        execstring = "select distinct " + colname + " from " + tablename
        self.cur.execute(execstring)
        col=self.cur.fetchall()
        return col               

    def getCol(self, variable, tablename, extracond = ''):
        execstring = "select " + variable + " from " + tablename + extracond
        self.cur.execute(execstring)
        cols=self.cur.fetchall()
        return cols

    def getCols(self, variable1, variable2, colname):
        execstring = "select " + variable1 + ", " + variable2 + " from " + tablename + "where name = '" + colname + "'"
        self.cur.execute(execstring)
        cols=self.cur.fetchall()
        return cols

    def getCount(self, variable, tablename, extracond = ''):
        execstring = "select " + variable + ", count(" + variable + ") from " + tablename + extracond + " group by " + variable + " order by " + variable
        self.cur.execute(execstring)
        cols=self.cur.fetchall()
        return cols

    def getHist(self, variable, tablename, nbins, minval, maxval, extracond = ''):
        # first get the maximum and minimum values 
        binwidth=(maxval-minval)/nbins
        execstring = "" 
        for i in range(nbins):
            minbin=str(minval+(i*binwidth))
            maxbin=str(minval+((i+1)*binwidth))

            if(i==0):
                extrastring = " select " + str(minval+((i+0)*binwidth)) + " as MidValue, count (" +variable + ") as Count from " + tablename + " where " + variable + ">=" + minbin + " and " + variable + " < " + maxbin + extracond + " union (" 
            else:
                extrastring = " select " + str(minval+((i+0)*binwidth)) + " as MidValue, count (" +variable + ") as Count from " + tablename + " where " + variable + ">=" + minbin + " and " + variable + " < " + maxbin + extracond + ") union (" 
            execstring=execstring+extrastring

        execstring = execstring[:-7]
        self.cur.execute(execstring)
        cols=self.cur.fetchall()
        return cols
