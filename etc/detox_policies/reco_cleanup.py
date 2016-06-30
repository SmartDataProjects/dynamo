from detox.policy import Policy
import detox.rules as rules

default = Policy.DEC_PROTECT

reco_cleanup = [
    rules.protect_incomplete,
    rules.DeleteRECOOlderThan(180., 'd'),
    rules.delete_partial
]
