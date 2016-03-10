import detox.policies as policies

policy_stack = [
    policies.deleteInvalid,
    policies.keepLocked,
    policies.deleteOld,
    policies.deleteExcess
]
