class Operation(object):
    """
    Defines the base class for a data operation (transfer or deletion).
    """

    # slots need to be defined to allow for the object to be loaded more quickly
    __slots__ = \
        ['rid', 'lfn', 'source', 'status', 'created', 'start', 'end', 'bid', 'size', 'ecode']

    def __init__(self):
        self.rid = 0
        self.lfn = ""
        self.source = ""
        self.status = ""
        self.created = ""
        self.start = 0
        self.end = 0
        self.bid = 0
        self.size = 0
        self.ecode = 0

    def __str__(self):
        return " (id: %s) %s %d %s %d %d %s %d"%\
            (self.rid,self.lfn,self.size,self.created,self.start,self.end,self.source,self.status)

    def fill(self,rid,lfn,source,status,created,start,end,bid,size,ecode):
        self.rid = rid
        self.lfn = lfn
        self.source = source
        self.status = status
        self.created = created
        self.start = -1
        if start is not None and type(start) is not int:
            self.start = int(start.strftime("%s"))
        self.end = -1
        if end is not None and type(end) is not int:
            self.end =  int(end.strftime("%s"))
        self.bid = bid
        self.size = size
        self.ecode = ecode

    def from_row(self,row,sites):
        if len(row) == 10:
            self.fill(int(row[0]),row[1],sites.names[int(row[2])],int(row[3]),
                      row[4],row[5],row[6],
                      row[7],int(row[8]),int(row[9]))
        else:
            print " ERROR - row length (%d) not compatible: %s"%(len(row),row)

class Deletion(Operation):
    """
    Defines the data operation deletion.
    """

    __slots__ = []

    def __init__(self):
        Operation.__init__(self)

    def __str__(self):
        return Operation.__str__(self)

    def fill(self,rid,lfn,source,status,created,start,end,bid,size,ecode):
        Operation.fill(self,rid,lfn,source,status,created,start,end,bid,size,ecode)

    def from_row(self,row,sites):
        Operation.from_row(self,row,sites)

    def show(self):
        print self.__str__()

class Transfer(Operation):

    __slots__ = [ 'target' ]

    def __init__(self):
        Operation.__init__(self)
        self.target = ""

    def __str__(self):
        return Operation.__str__(self) + " to target: %s"%(self.target)

    def fill(self,rid,lfn,source,target,status,created,start,end,bid,size,ecode):
        Operation.fill(self,rid,lfn,source,status,created,start,end,bid,size,ecode)
        self.target = target

    def from_row(self,row,sites):
        if len(row) == 11:
            self.fill(int(row[0]),row[1],sites.names[int(row[2])],sites.names[int(row[3])],
                      int(row[4]),row[5],row[6],row[7],row[8],int(row[9]),int(row[10]))
        else:
            print " ERROR Transfer - row length (%d) not compatible: %s"%(len(row),row)

    def show(self):
        print self.__str__()
