import logging

from dataformat import ConfigurationError
import policy.variables as variables
import policy.attrs as attrs
import policy.predicates as predicates
import demand
from detox.conditions import ReplicaCondition, SiteCondition
from detox.sort import SortKey

LOG = logging.getLogger(__name__)

class Decision(object):
    """Generator of decisions. An instance of cls is created for each replica."""

    def __init__(self, cls, *common_args):
        self.action_cls = cls
        self.common_args = common_args

    def action(self, matched_line, *args):
        return self.action_cls(matched_line, *(args + self.common_args))

class Action(object):
    def __init__(self, matched_line):
        self.matched_line = matched_line

class DatasetAction(Action):
    def __init__(self, matched_line):
        Action.__init__(self, matched_line)

class Protect(DatasetAction):
    pass

class Delete(DatasetAction):
    pass

class Dismiss(DatasetAction):
    pass

class BlockAction(Action):
    def __init__(self, matched_line, block_replicas = []):
        Action.__init__(self, matched_line)

        self.block_replicas = set(block_replicas)

class ProtectBlock(BlockAction):
    @staticmethod
    def dataset_level(matched_line, *args):
        return Protect(matched_line)

class DeleteBlock(BlockAction):
    @staticmethod
    def dataset_level(matched_line, *args):
        return Delete(matched_line)

class DismissBlock(BlockAction):
    @staticmethod
    def dataset_level(matched_line, *args):
        return Dismiss(matched_line)

class PolicyLine(object):
    """
    Call this Policy when fixing the terminology.
    AND-chained list of predicates.
    """

    def __init__(self, decision, text):
        self.condition = ReplicaCondition(text)
        self.decision = decision
        self.has_match = False

        # filled by history interface
        self.condition_id = 0

    def __str__(self):
        return self.condition.text

    def evaluate(self, replica):
        action = None

        if self.condition.match(replica):
            self.has_match = True

            if issubclass(self.decision.action_cls, BlockAction):
                # block-level
                matching_block_replicas = self.condition.get_matching_blocks(replica)
                if len(matching_block_replicas) == len(replica.block_replicas):
                    # but all blocks matched - return dataset level
                    action = self.decision.action_cls.dataset_level(self)
                else:
                    action = self.decision.action(self, matching_block_replicas)
            else:
                action = self.decision.action(self)

        return action


class DetoxPolicy(object):
    def __init__(self, config):
        self.demand_plugins = []
        
        LOG.info('Reading the policy file.')
        with open(config.policy_file) as policy_def:
            self.parse_lines(policy_def, config.demand)
        
        # Special config - shift time-based policies by config.time_shift days for simulation
        if config.get('time_shift', 0.) > 0.:
            for line in self.policy_lines:
                for pred in line.condition.predicates:
                    if type(pred) is predicates.BinaryExpr and pred.variable.vtype == attrs.Attr.TIME_TYPE:
                        pred.rhs += config.time_shift * 24. * 3600.

        # Iterative deletion can be turned off in specific policy files. When this is False,
        # Detox will finalize the delete and protect list in the first iteration.
        self.iterative_deletion = True

        self.version = config.policy_version

        # Check if the replicas can be deleted just before making the deletion requests.
        # Set to a function that takes a list of dataset replicas and removes from it
        # the replicas that should not be deleted.
        self.predelete_check = None

    def parse_lines(self, lines, demand_config):
        LOG.info('Parsing policy stack.')

        if type(lines) is file:
            conf = lines
            lines = map(str.strip, conf.read().split('\n'))
            il = 0
            while il != len(lines):
                if lines[il] == '' or lines[il].startswith('#'):
                    lines.pop(il)
                else:
                    il += 1

        self.partition_name = ''
        self.target_site_def = []
        self.deletion_trigger = []
        self.stop_condition = []
        self.policy_lines = []
        self.default_decision = None
        self.candidate_sort_key = None

        LINE_PARTITION, LINE_SITE_TARGET, LINE_DELETION_TRIGGER, LINE_STOP_CONDITION, LINE_POLICY, LINE_ORDER = range(6)

        for line in lines:
            line_type = -1

            words = line.split()
            if words[0] == 'Partition':
                line_type = LINE_PARTITION
            elif words[0] == 'On':
                line_type = LINE_SITE_TARGET
            elif words[0] == 'When':
                line_type = LINE_DELETION_TRIGGER
            elif words[0] == 'Until':
                line_type = LINE_STOP_CONDITION
            elif words[0] == 'Order':
                line_type = LINE_ORDER
            elif words[0] in ('Protect', 'Delete', 'Dismiss', 'ProtectBlock', 'DeleteBlock', 'DismissBlock'):
                line_type = LINE_POLICY
                decision = Decision(eval(words[0]))
            else:
                raise ConfigurationError(line)

            if line_type == LINE_PARTITION:
                self.partition_name = words[1]

            else:
                cond_text = ' '.join(words[1:])

                if line_type == LINE_SITE_TARGET:
                    self.target_site_def.append(SiteCondition(cond_text))

                elif line_type == LINE_DELETION_TRIGGER:
                    self.deletion_trigger.append(SiteCondition(cond_text))

                elif line_type == LINE_STOP_CONDITION:
                    self.stop_condition.append(SiteCondition(cond_text))

                elif line_type == LINE_POLICY:
                    if len(words) == 1:
                        self.default_decision = decision
                    else:
                        self.policy_lines.append(PolicyLine(decision, cond_text))

                elif line_type == LINE_ORDER:
                    self.candidate_sort_key = SortKey(cond_text)

        if self.partition_name == '':
            raise ConfigurationError('Partition name missing.')
        if len(self.target_site_def) == 0:
            raise ConfigurationError('Target site definition missing.')
        if len(self.deletion_trigger) == 0 or len(self.stop_condition) == 0:
            raise ConfigurationError('Deletion trigger and release expressions are missing.')
        if self.default_decision == None:
            raise ConfigurationError('Default decision not given.')

        # Collect demand plugin class names from all conditions and sortkey, instantiate the plugins
        demand_class_names = set()

        for conds in [self.target_site_def, self.deletion_trigger, self.stop_condition]:
            for cond in conds:
                demand_class_names.update(cond.demand_classes)

        for line in self.policy_lines:
            demand_class_names.update(line.condition.demand_classes)

        demand_class_names.update(self.candidate_sort_key.demand_classes)

        for cls_name in demand_class_names:
            self.demand_plugins.append(getattr(demand, cls_name)(demand_config[cls_name]))

        LOG.info('Policy stack for %s: %d lines using demand plugins %s', self.partition_name, len(self.policy_lines), str(sorted(self.demand_plugins)))

    def evaluate(self, replica):
        actions = []
        block_replicas_tmp = set()

        for line in self.policy_lines:
            action = line.evaluate(replica)
            if action is None:
                continue

            actions.append(action)
            if isinstance(action, DatasetAction):
                break

            else:
                # strip the block replicas from dataset replica so the successive
                # policy lines don't see them any more
                for block_replica in action.block_replicas:
                    replica.block_replicas.remove(block_replica)
                    block_replicas_tmp.add(block_replica)

        else:
            actions.append(self.default_decision.action(None))

        # return the block replicas
        replica.block_replicas.update(block_replicas_tmp)
        
        return actions
