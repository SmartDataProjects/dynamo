from detox.policy import Policy
import detox.rules as rules

default = Policy.DEC_PROTECT

exceptions = rules.ActionList()
exceptions.add_action('Keep', '*', '/HLTPhysics/CMSSW_7_4_14-2015_10_20_newconditions0_74X_dataRun2_HLTValidation_Candidate_2015_10_12_10_41_09-v1/RECO')
exceptions.add_action('Keep', '*', '/HLTPhysics/CMSSW_7_4_14-2015_10_20_reference_74X_dataRun2_HLT_v2-v1/RECO')
exceptions.add_action('Keep', '*', '/ZeroBias*/Run2015A-PromptReco-v1/RECO')
exceptions.add_action('Keep', '*', '/*TOTEM*/*Run2015D*/RECO')

reco_cleanup = [
    rules.protect_incomplete,
    exceptions,
    rules.DeleteRECOOlderThan(180., 'd'),
    rules.delete_partial
]
