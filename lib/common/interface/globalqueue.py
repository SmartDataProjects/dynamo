from common.interface.jobqueue import JobQueue
from common.interface.htcondor import HTCondor
import common.configuration as config

class GlobalQueue(JobQueue):
    """
    Interface to CMS Global Queue.
    """

    def __init__(self):
        self.htcondor = HTCondor(config.globalqueue.collector, schedd_constraint = config.globalqueue.schedd_constraint)

        
