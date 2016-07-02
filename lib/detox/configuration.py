from common.configuration import paths, Configuration

delete_old = Configuration()
delete_old.threshold = (1.5, 'y')

delete_unpopular = Configuration()
delete_unpopular.threshold = 1.

deletion_per_iteration = 3

reco_max_age = 180. # number of days to keep /RECO* datasets
max_nonusage = 500. # threshold for global usage rank

routine_exceptions = [
    ('Keep', '*', '/HLTPhysics/CMSSW_7_4_14-2015_10_20_newconditions0_74X_dataRun2_HLTValidation_Candidate_2015_10_12_10_41_09-v1/RECO'),
    ('Keep', '*', '/HLTPhysics/CMSSW_7_4_14-2015_10_20_reference_74X_dataRun2_HLT_v2-v1/RECO'),
    ('Keep', '*', '/ZeroBias*/Run2015A-PromptReco-v1/RECO'),
    ('Keep', '*', '/*TOTEM*/*Run2015D*/RECO')
]
