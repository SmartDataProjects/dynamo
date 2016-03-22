import detox.policies as policies

policy_stack = {
    'TargetFraction': [
        policies.keepTargetOccupancy,
        policies.keepIncomplete,
        policies.keepLocked,
        policies.keepCustodial,
        policies.keepDiskOnly,
        policies.deletePartial,
        policies.deleteOld,
        policies.deleteUnpopular
    ],
    'Greedy': [
        policies.keepIncomplete,
        policies.keepLocked,
        policies.keepCustodial,
        policies.keepDiskOnly
    ] # add actual deletion policies
}
